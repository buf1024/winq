from typing import Optional
from datetime import datetime
import pandas as pd
import hiq_pyfetch as fetch
from winq.selector.strategy.strategy import Strategy


class SuperFalling(Strategy):
    """
    热门超跌上涨。
    实战：10:30 20cm涨幅不高于5%，回撤不大于2%，10cm涨幅不超过3%，回撤不大于1%，且当前不是最低，缓慢上涨。
    选股：当日下跌，高点应该5日内出现，高点到当日，回撤2~10个点，最高点前连续上涨（红盘或大于-0.5%），累计上涨大于10%
    如2023/03/29 688313。
    前一晚，应该选择符合条件的票，10:20计算进行实盘交易。
    """

    def __init__(self, db, **kwargs):
        super().__init__(db, **kwargs)
        self.max_first_day_rise = 3.0
        self.max_first_day_down = -6.0
        self.max_down_as_rise = -0.5
        self.max_meet_high_days = 5
        self.min_high_falling = 2.0
        self.max_high_falling = 10.0
        self.max_turnover = 18.0
        self.min_rise_to_high_ndays = 10.0
        self.max_rise_unfill_gaps = 5.0

    @staticmethod
    def desc():
        return '  名称: 热股超跌(基于日线)\n' + \
               '  说明: 热股超跌股票次日交易\n' + \
               '  参数: max_down_as_rise -- 下跌多少视为上涨(默认: -0.5)\n' + \
               '        max_first_day_rise -- 当日最大涨幅(默认: 3.0)\n' + \
               '        max_first_day_down -- 当日最大跌幅幅(默认: 6.0)\n' + \
               '        max_meet_high_days -- 当日下跌到前高点天数(默认: 5)\n' + \
               '        min_high_falling -- 最高点最小下跌百分比(默认: 2.0)\n' + \
               '        max_high_falling -- 最高点最大下跌百分比(默认: 10.0)\n' + \
               '        max_turnover -- 最大换手率(默认: 18.0)\n' + \
               '        min_rise_to_high_ndays -- 最高点前连续上涨最小百分比(默认: 10.0)\n' + \
               '        max_rise_unfill_gaps -- 未回补的最大跳空缺口(默认: 5.0)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'max_first_day_rise' in kwargs:
                self.max_first_day_rise = float(kwargs['max_first_day_rise'])
            if kwargs is not None and 'max_first_day_down' in kwargs:
                self.max_first_day_down = float(kwargs['max_first_day_down'])
            if kwargs is not None and 'max_down_as_rise' in kwargs:
                self.max_down_as_rise = float(kwargs['max_down_as_rise'])
            if kwargs is not None and 'max_meet_high_days' in kwargs:
                self.max_meet_high_days = int(kwargs['max_meet_high_days'])
            if kwargs is not None and 'min_high_falling' in kwargs:
                self.min_high_falling = float(kwargs['min_high_falling'])
            if kwargs is not None and 'max_high_falling' in kwargs:
                self.max_high_falling = float(kwargs['max_high_falling'])
            if kwargs is not None and 'max_turnover' in kwargs:
                self.max_turnover = float(kwargs['max_turnover'])
            if kwargs is not None and 'min_rise_to_high_ndays' in kwargs:
                self.min_rise_to_high_ndays = float(kwargs['min_rise_to_high_ndays'])
            if kwargs is not None and 'max_rise_unfill_gaps' in kwargs:
                self.max_rise_unfill_gaps = float(kwargs['max_rise_unfill_gaps'])

        except ValueError:
            self.log.error('策略参数不合法')
            return False
        self.is_prepared = True
        return self.is_prepared

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        kdata = await self.load_kdata(code=code,
                                      filter={'code': code,
                                              'trade_date': {'$lte': self.test_end_date}},
                                      limit=self.min_trade_days,
                                      sort=[('trade_date', -1)])

        if kdata is None or kdata.shape[0] < self.min_trade_days:
            return None
        
        data0 = kdata.iloc[0]
        data0_chg_pct = data0['chg_pct']
        if data0_chg_pct >= self.max_first_day_rise or \
                data0_chg_pct <= self.max_first_day_down:
            return None

        data0_close = data0['close']
        max_index = 0
        for i in range(self.max_meet_high_days):
            max_close = kdata.iloc[max_index]['close']
            cur = kdata.iloc[i]
            cur_close, cur_turnover = cur['close'], cur['turnover']
            if cur_close > max_close:
                max_index = i
            if cur_turnover > self.max_turnover:
                return None

        if max_index == 0:
            return None

        max_close = kdata.iloc[max_index]['close']
        high_falling_down = round(
            (data0_close - max_close) / max_close * 100, 2)
        falling_down_abs = abs(high_falling_down)
        if falling_down_abs < self.min_high_falling or falling_down_abs > self.max_high_falling:
            return None

        rise_index = max_index
        for i in range(max_index, self.min_trade_days):
            df = kdata.iloc[i]
            close, chg_pct, turnover = df['close'], df['chg_pct'], df['turnover']
            if turnover > self.max_turnover:
                return None
            if (self.is_red(df) or chg_pct > self.max_down_as_rise) and max_close >= close:
                rise_index = i
                continue
            break
        if rise_index == max_index:
            return None

        rise_close = kdata.iloc[rise_index]['close']
        high_rise = abs((max_close - rise_close) / rise_close * 100)
        if high_rise < self.min_rise_to_high_ndays:
            return None
        
        # 耗时操作
        kdata = self.cal_gaps(kdata=kdata)
        
        gap_kdata = kdata[(kdata['gap_pct'] > self.max_rise_unfill_gaps) & (kdata['is_gap_fill'] == False)]
        if len(gap_kdata) > 0:
            return None

        got_data = dict(code=code, name=name,
                        chg_pct=data0_chg_pct,
                        high_falling_days=max_index,
                        high_falling_down=high_falling_down,
                        high_rise_days=rise_index - max_index,
                        high_rise=high_rise)
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from winq import *

    db = default(log_level='debug')

    async def test_select():
        s = SuperFalling(db=db,
                         test_end_date='20230406',
                         with_rt_kdata=False,
                         run_task_count=50,
                         load_daily=db.load_stock_daily,
                         load_info=db.load_stock_info,
                         fetch_daily=fetch.fetch_stock_bar
                         )

        await s.prepare(max_first_day_rise=3.0,
                        max_down_as_rise=-1.5,
                        max_meet_high_days=10,
                        min_high_falling=2.0,
                        max_high_falling=20.0,
                        max_turnover=18.0,
                        min_rise_to_high_ndays=10.0,
                        max_rise_unfill_gaps=5.0,
                        sort_by='chg_pct')

        data = await s.test(code='sz300275')
        # data = await s.test(code='sh688047')*
        # data = await s.test(code='sh688313')

        print(data)

    run_until_complete(test_select())
