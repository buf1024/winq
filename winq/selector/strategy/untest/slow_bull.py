from cgi import test
from typing import Optional
import pandas as pd
from winq.selector.strategy.strategy import Strategy


class SlowBull(Strategy):
    """
    慢牛股票：
    1. 机构持股远高于散户，表现为低换手率
    2. 持续上涨，涨不停
    3. 符合当前风口
    """

    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.max_turnover = 3.0
        self.min_up_down = 5.0

    @staticmethod
    def desc():
        return '  名称: 低换手率持续上涨选股(基于日线)\n' + \
               '  说明: 慢牛股票\n' + \
               '  参数: max_turnover -- 最大换手率(默认: 3)\n' + \
               '        min_up_down -- 最小涨跌幅度(默认: 5)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'max_turnover' in kwargs:
                self.max_turnover = float(kwargs['max_turnover'])
            if kwargs is not None and 'min_up_down' in kwargs:
                self.min_up_down = float(kwargs['min_up_down'])
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

        if kdata is None or kdata.empty:
            return None
        
        if kdata['turnover'].mean() > self.max_turnover:
            return None
        
        r = (kdata.iloc[0]['close'] - kdata.iloc[-1]['close'])*100 / kdata.iloc[-1]['close']
        if r <= 3.0:
            return None
        
        
        test_data = kdata[(kdata['rise'] > self.min_up_down) |
                         (kdata['rise'] < -self.min_up_down)]
        
        if len(test_data) <= 0:
            return None
        
        got_data = test_data[test_data['turnover'] < self.max_turnover]
        if len(got_data)*1.0 < len(test_data)*0.7:
            return None
        
        
        name = await self.code_name(code=code, name=name)
        got_data = dict(code=code, name=name,
                        mean_turnover=kdata['turnover'].mean(),
                        max_turnover=kdata['turnover'].max(),
                        min_turnover=kdata['turnover'].min())
        
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from winq import *
    from datetime import datetime

    fund, stock, mysql = default(log_level='error')
    s = SlowBull(db=stock, test_end_date='20220815')


    async def tt():
        await s.prepare()
        df = await s.test(code='sh600549')
        print(df)


    run_until_complete(tt())
