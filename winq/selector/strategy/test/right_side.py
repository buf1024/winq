from typing import Optional
import pandas as pd
import hiq_pyfetch as fetch
from winq.selector.strategy.strategy import Strategy


class RightSide(Strategy):
    """
    看上方有没有抛压，上涨幅度。
    示意形态:
       |
      |
     |
    |
    """

    def __init__(self, db, **kwargs):
        super().__init__(db, **kwargs)
        self.max_down_in_rise = -2.0
        self.max_up_in_rise = 20.0
        self.max_falling_down = 2.0
        self.max_turnover = 8.0
        self.min_high_ndays = 20
        self.test_recent_ndays = 8
        self.test_recent_ndays_up = 15.0

    @staticmethod
    def desc():
        return '  名称: 右侧选股(基于日线)\n' + \
               '  说明: 选择右侧上涨的股票\n' + \
               '  参数: max_down_in_rise -- 最大下跌百分比(默认: -2.0)\n' + \
               '        max_up_in_rise -- 最大上涨百分比(默认: 20.0)\n' + \
               '        max_falling_down -- 天线下跌百分比(默认: 2.0)\n' + \
               '        max_turnover -- 最大换手率(默认: 8.0)\n' + \
               '        min_high_ndays -- 当前日是否n日的最高点(默认: 20)\n' + \
               '        test_recent_ndays -- 最近累计计算涨幅天数(默认: 8)\n' + \
               '        test_recent_ndays_up -- 最近test_recent_ndays内上涨百分比(默认: 15.0)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'max_down_in_rise' in kwargs:
                self.max_down_in_rise = float(kwargs['max_down_in_rise'])
            if kwargs is not None and 'max_up_in_rise' in kwargs:
                self.max_up_in_rise = float(kwargs['max_up_in_rise'])
            if kwargs is not None and 'max_falling_down' in kwargs:
                self.max_falling_down = float(kwargs['max_falling_down'])
            if kwargs is not None and 'max_turnover' in kwargs:
                self.max_turnover = float(kwargs['max_turnover'])
            if kwargs is not None and 'min_high_ndays' in kwargs:
                self.min_high_ndays = int(kwargs['min_high_ndays'])
            if kwargs is not None and 'test_recent_ndays' in kwargs:
                self.test_recent_ndays = int(kwargs['test_recent_ndays'])
                if self.test_recent_ndays <= 0 or self.test_recent_ndays > self.min_trade_days:
                    self.log.error('策略参数test_recent_ndays不合法: {}~{}'.format(
                        0, self.min_trade_days))
                    return False
            if kwargs is not None and 'test_recent_ndays_up' in kwargs:
                self.test_recent_ndays_up = float(
                    kwargs['test_recent_ndays_up'])

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
        n_close = data0['close']

        high_days = 0
        high_days_break = False

        hit_days = 0
        hit_days_break = False
        for i in range(len(kdata)):
            df = kdata.iloc[i]
            chg_pct, turnover, close = df['chg_pct'], df['turnover'], df['close']
            falling_down = self.falling_down(df=df)
            
            if not high_days_break:
                if n_close >= close:
                    high_days = high_days + 1
                else:
                    high_days_break = True
            
            if not hit_days_break:
                if self.max_down_in_rise <= chg_pct <= self.max_up_in_rise and \
                        falling_down <= self.max_falling_down and \
                        turnover <= self.max_turnover:
                    hit_days = hit_days + 1
                else:
                    hit_days_break = True
            
            if high_days_break and hit_days_break:
                break

        if hit_days < self.min_hit_days or high_days < self.min_high_ndays:
            return None

        rn_close = kdata.iloc[self.test_recent_ndays]['close']
        recent_ndays_up = round((n_close - rn_close) * 100 / rn_close, 2)
        if recent_ndays_up >= self.test_recent_ndays_up:
            hit_data = kdata.iloc[hit_days-1]
            pre_close = hit_data['close'] / (1+hit_data['chg_pct']/100.0)
            hit_chg_pct = round(
                (data0['close'] - pre_close)*100 / pre_close, 2)
            got_data = dict(code=code, name=name,
                            nday_close=rn_close,
                            close=n_close,
                            nday_chg_pct=recent_ndays_up,
                            hit_start=hit_data['trade_date'],
                            hit_chg_pct=hit_chg_pct,
                            hit_days=hit_days)
            return pd.DataFrame([got_data])

        return None


if __name__ == '__main__':
    from winq import *

    db = default(log_level='debug')

    async def test_concept():
        s = RightSide(db=db,
                      test_end_date='20230322',
                      with_rt_kdata=False,
                      run_task_count=50,
                      load_daily=db.load_stock_concept_daily,
                      load_info=db.load_stock_concept,
                      fetch_daily=fetch.fetch_stock_concept_daily
                      )
        await s.prepare(min_hit_days=2,
                        max_down_in_rise=0.0,
                        max_up_in_rise=5.0,
                        max_falling_down=2.0,
                        max_turnover=10.0,
                        min_high_ndays=3,
                        test_recent_ndays=2,
                        test_recent_ndays_up=3.0,
                        sort_by='hit_chg_pct')
        data = await s.test(code='BK0959')
        print(data)

    async def test_stock():
        s = RightSide(db=db,
                      test_end_date=None,
                      with_rt_kdata=True,
                      run_task_count=50,
                      load_daily=db.load_stock_daily,
                      load_info=db.load_stock_info,
                      fetch_daily=fetch.fetch_stock_bar
                      )

        await s.prepare(min_hit_days=1,
                        max_down_in_rise=3.0,
                        max_up_in_rise=20.0,
                        max_falling_down=3.0,
                        max_turnover=10.0,
                        min_high_ndays=2,
                        test_recent_ndays=1,
                        test_recent_ndays_up=0.0,
                        sort_by='hit_chg_pct')

        data = await s.test(code='sh600586')
        print(data)

    run_until_complete(test_stock())
