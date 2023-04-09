from typing import Optional
import pandas as pd
from winq.selector.strategy.strategy import Strategy


class ShockRise(Strategy):
    """
    左侧震荡后，开始突破
    示意形态:
         |
        |
    ||||
    """

    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.min_break_days = 3
        self.is_con_break_up = True
        self.max_leg_ratio = 33.3
        self.min_break_up = 5.0
        self.max_break_con_up = 3.0
        self.max_break_leg_ratio = 33.33
        self.min_shock_days = 15
        self.max_con_shock = 10.0
        self.max_shock = 15.0

    @staticmethod
    def desc():
        return '  名称: 底部横盘突破选股(基于日线)\n' + \
               '  说明: 选择底部横盘的股票\n' + \
               '  参数: min_break_days -- 最近突破上涨天数(默认: 3)\n' + \
               '        is_con_break_up -- 涨幅是否持续放大(默认: True)\n' + \
               '        min_break_up -- 最近累计突破上涨百分比(默认: 5.0)\n' + \
               '        max_break_con_up -- 最近突破上涨百分比(默认: 3.0)\n' + \
               '        max_break_leg_ratio -- 上下天线最大比例(默认: 33.33)\n' + \
               '        min_shock_days -- 最小横盘天数(默认: 15)\n' + \
               '        max_con_shock -- 横盘天数内隔天波动百分比(默认: 10.0)\n' + \
               '        max_shock -- 横盘天数内总波动百分比(默认: 15.0)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'min_trade_days' in kwargs:
                self.min_trade_days = int(kwargs['min_trade_days'])
            if kwargs is not None and 'min_break_days' in kwargs:
                self.min_break_days = int(kwargs['min_break_days'])
            if kwargs is not None and 'min_break_days' in kwargs:
                self.is_con_break_up = kwargs['min_break_days']
            if kwargs is not None and 'min_break_up' in kwargs:
                self.min_break_up = float(kwargs['min_break_up'])
            if kwargs is not None and 'max_break_con_up' in kwargs:
                self.max_break_con_up = float(kwargs['max_break_con_up'])
            if kwargs is not None and 'max_break_leg_ratio' in kwargs:
                self.max_break_leg_ratio = float(kwargs['max_break_leg_ratio'])
            if kwargs is not None and 'min_shock_days' in kwargs:
                self.min_shock_days = int(kwargs['min_shock_days'])
                if self.min_trade_days <= 0 or self.min_shock_days > self.min_trade_days:
                    self.log.error('策略参数min_shock_days不合法: {}~{}'.format(0, self.min_trade_days))
                    return False
            if kwargs is not None and 'max_con_shock' in kwargs:
                self.max_con_shock = float(kwargs['max_con_shock'])
            if kwargs is not None and 'max_shock' in kwargs:
                self.max_shock = float(kwargs['max_shock'])

        except ValueError:
            self.log.error('策略参数不合法')
            return False
        self.is_prepared = True
        return self.is_prepared

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:

        kdata = await self.load_kdata(filter={'code': code,
                                              'trade_date': {'$lte': self.test_end_date}},
                                      limit=self.min_trade_days,
                                      sort=[('trade_date', -1)])

        if kdata is None or kdata.shape[0] < self.min_break_days + self.min_shock_days:
            return None

        test_data = kdata[:self.min_break_days]
        fit_days = 0
        pre_rise = 0
        for df in test_data.to_dict('records'):
            rise = df['rise']
            if rise >= self.max_break_con_up:
                if self.is_long_leg(df, self.max_break_leg_ratio):
                    break
                if pre_rise == 0:
                    fit_days = fit_days + 1
                    continue
                if self.is_con_break_up and rise <= pre_rise:
                    fit_days = fit_days + 1
                    continue
            break

        if fit_days < self.min_break_days:
            return None

        break_rise = test_data['rise'].sum()
        if break_rise < self.min_break_up:
            return None

        break_days = fit_days
        break_date = kdata.iloc[fit_days]['trade_date']

        test_data = kdata[self.min_break_days:]

        fit_days = 0
        for df in test_data.to_dict('records'):
            rise = abs(df['rise'])
            if rise <= self.max_con_shock:
                fit_days = fit_days + 1
                continue
            break

        if fit_days < self.min_shock_days:
            return None

        hor_close, pre_hor_close = test_data.iloc[0]['close'], kdata.iloc[self.min_shock_days]['close']
        rise = round((hor_close - pre_hor_close) * 100 / pre_hor_close, 2)
        if abs(rise) > self.max_shock:
            return None

        name = await self.code_name(code=code, name=name)
        got_data = dict(code=code, name=name,
                        close=kdata.iloc[0]['close'],
                        break_date=break_date, break_days=break_days, break_rise=break_rise,
                        shock_days=fit_days, shock_rise=rise)
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from winq import *
    from datetime import datetime

    fund, stock, mysql = default(log_level='error')
    s = ShockRise(db=stock, test_end_date='20211228')


    async def tt():
        await s.prepare(min_break_days=2, min_break_up=4.0, max_break_con_up=0.5, max_break_leg_ratio=33.33,
                        min_shock_days=15, max_con_shock=4.0, max_shock=15.0)
        df = await s.test(code='sh600118')
        print(df)


    run_until_complete(tt())
