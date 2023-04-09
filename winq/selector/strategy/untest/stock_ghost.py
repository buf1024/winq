from typing import Optional
import pandas as pd
from winq.selector.strategy.strategy import Strategy


class StockGhost(Strategy):
    """
    每年11月份都是可能产生妖股的时间
    妖股特点： 低价  亏损  市值  热点
    """

    def __init__(self, db, *, test_end_date=None):
        super().__init__(db, test_end_date=test_end_date, skip_st=False)
        self.max_mv = 50.0
        self.max_price = 6.0
        self.max_profit = 0
        self.min_lost_season = 4
        # 热点？

    @staticmethod
    def desc():
        return '  名称: 11月份抓妖股。\n' \
               '  妖股特点： 低价 亏损  低市值 热点\n' + \
               '  说明: 选择可能出现的妖股\n' + \
               '  参数: max_mv -- 最大市值(默认: 50亿)\n' + \
               '        max_price -- 最大股价(默认: 6.0)\n' + \
               '        max_profit -- 最大净利润(默认: 0.0，即亏损)\n' + \
               '        min_lost_season -- 持续亏损季度(默认: 4)\n'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'max_mv' in kwargs:
                self.max_mv = float(kwargs['max_mv'])
            if kwargs is not None and 'max_price' in kwargs:
                self.max_price = float(kwargs['max_price'])
            if kwargs is not None and 'max_profit' in kwargs:
                self.max_profit = float(kwargs['max_profit'])
            self.max_mv = self.max_mv * 10000
        except ValueError:
            self.log.error('策略参数不合法')
            return False
        self.sort_by = 'total_mv'
        self.sort_reverse = False
        self.is_prepared = True
        return self.is_prepared

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:

        index_data = await self.db.load_stock_index(
            filter={'code': code, 'total_mv': {'$lte': self.max_mv}},
            limit=1,
            sort=[('trade_date', -1)])

        if index_data is None or index_data.empty:
            return None

        kdata = await self.load_kdata(
            filter={'code': code,
                    'trade_date': {'$lte': self.test_end_date}},
            limit=1,
            sort=[('trade_date', -1)])

        if kdata is None or kdata.empty:
            return None
        if kdata.iloc[0]['close'] > self.max_price:
            return None

        profit_data = await self.db.load_stock_yjbb(
            filter={'code': code[2:], 'season_date': {'$lte': self.test_end_date}},
            limit=self.min_lost_season,
            sort=[('season_date', -1)])

        if profit_data is None or profit_data.empty:
            return None
        if len(profit_data) < self.min_lost_season:
            return None
        for i in range(self.min_lost_season):
            if profit_data.iloc[i]['jlr'] > self.max_profit:
                return None

        name = await self.code_name(code=code, name=name)
        trade_date, total_mv = kdata.iloc[0]['trade_date'], index_data.iloc[0]['total_mv']
        close = kdata.iloc[0]['close']
        profit = ''
        for i in range(self.min_lost_season):
            profit = profit + profit_data.iloc[i]['season_date'].strftime('%Y') + '\n' \
                     + str(round(profit_data.iloc[i]['jlr']/10000.0, 2)) + '万'

        got_data = dict(code=code, name=name,
                        trade_date=trade_date, total_mv=total_mv, close=close, profit=profit)

        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from winq import *

    fund, stock, bond, mysql = default(log_level='debug')
    s = StockGhost(db=stock)


    async def tt():
        await s.prepare(max_price=40.0)
        df = await s.test('sz003007')
        print(df)


    run_until_complete(tt())
