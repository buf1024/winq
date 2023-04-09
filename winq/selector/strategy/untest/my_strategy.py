from typing import Optional
import pandas as pd
from winq.selector.strategy.strategy import Strategy


class MyStrategy(Strategy):
    def __init__(self, db, *, test_end_date=None):
        super().__init__(db, test_end_date=test_end_date)
        self.min_rise_days = 3
        self.max_rise_per_day = 9.0
        
        self.min_shock_days = 22
        self.max_shock_per_day = 5.0
        self.max_shock = 15.0

    @staticmethod
    def desc():
        return '  名称: 右侧选股(基于日线)\n' + \
               '  说明: 选择右侧上涨的股票\n' + \
               '  参数: min_rise_days -- 最近最小连续上涨天数(默认: 3)\n' + \
               '        max_down_in_rise -- 最大下跌百分比(默认: -2.0)\n' + \
               '        max_up_in_rise -- 最大上涨百分比(默认: 20.0)\n' + \
               '        max_leg_ratio -- 上涨最大腿长(默认: 33.3)\n' + \
               '        recent_days -- 最近累计计算涨幅天数(默认: 8)\n' + \
               '        recent_days_up -- 最近judge_days内上涨百分比(默认: 15.0)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        # try:
        #     if kwargs is not None and 'min_rise_days' in kwargs:
        #         self.min_rise_days = int(kwargs['min_rise_days'])
        #     if kwargs is not None and 'max_down_in_rise' in kwargs:
        #         self.max_down_in_rise = float(kwargs['max_down_in_rise'])
        #     if kwargs is not None and 'max_up_in_rise' in kwargs:
        #         self.max_up_in_rise = float(kwargs['max_up_in_rise'])
        #     if kwargs is not None and 'max_leg_ratio' in kwargs:
        #         self.max_leg_ratio = float(kwargs['max_leg_ratio'])
        #     if kwargs is not None and 'recent_ndays' in kwargs:
        #         self.recent_ndays = int(kwargs['recent_ndays'])
        #         if self.recent_ndays <= 0 or self.recent_ndays > self.min_trade_days:
        #             self.log.error('策略参数recent_ndays不合法: {}~{}'.format(0, self.min_trade_days))
        #             return False
        #     if kwargs is not None and 'recent_days_up' in kwargs:
        #         self.recent_days_up = float(kwargs['recent_days_up'])

        # except ValueError:
        #     self.log.error('策略参数不合法')
        #     return False
        self.is_prepared = True
        return self.is_prepared

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        if self.skip_kcb and code.startswith('sh688'):
            return None

        kdata = await self.load_kdata(filter={'code': code,
                                              'trade_date': {'$lte': self.test_end_date}},
                                      limit=self.min_trade_days,
                                      sort=[('trade_date', -1)])

        if kdata is None or kdata.shape[0] < self.min_trade_days:
            return None

        fit_days = 0
        for df in kdata.to_dict('records'):
            rise = df['rise']
            if (0 <= rise <= self.max_rise_per_day and df['close'] >= df['open']) \
                and df['vol_diff'] > 0:
                fit_days = fit_days + 1
                continue
            break

        if fit_days < self.min_rise_days:
            return None
        
        shock_days, max_shock = 0, 0
        low, high = 999999, -999999
        now_price = kdata.iloc[0]['close']
        for df in kdata[self.min_rise_days:].to_dict('records'):
            test_close = df['close']
            if test_close > now_price:
                break
            
            if low > test_close:
                low = test_close
            if high < test_close:
                high = test_close
            max_shock = round((high - low) / low * 100, 2)
            if max_shock > self.max_shock:
                break
            
            rise = abs(df['rise'])
            if rise <= self.max_shock_per_day:
                shock_days = shock_days + 1
                continue
            
            break
        
        if shock_days < self.min_shock_days:
            return None
        
        max_shock = round((high - low) / low * 100, 2)

        name = await self.code_name(code=code, name=name)
        got_data = dict(code=code, name=name,
                        rise_start=kdata.iloc[fit_days]['trade_date'], fit_days=fit_days,
                        shock_days=shock_days, max_shock=max_shock)
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from winq import *

    fund, stock, mysql = default(log_level='error')
    s = MyStrategy(db=stock, test_end_date='2021-10-29')

    async def tt():
        await s.prepare()
        # df = await s.test('sh600318')
        df = await s.test('sh60383')
        print(df)

    run_until_complete(tt())
