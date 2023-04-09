import random
from typing import Optional
import pandas as pd
from winq.selector.strategy.strategy import Strategy


class RandCode(Strategy):
    def __init__(self, db, *, test_end_date=None, select_count=999999):
        super().__init__(db, test_end_date=test_end_date, select_count=select_count)
        self.market = None
        self.db_load_func = None

    async def prepare(self, **kwargs):
        await super().prepare(**kwargs)

        self.market = kwargs['market'] if kwargs is not None and 'market' in kwargs else None

        if self.market is not None:
            if self.market != 'fund' and self.market != 'stock':
                self.log.error('策略参数 market={} 不正确, 值为: sz 或 sh'.format(self.market))
                return False

        self.db_load_func = self.db.load_stock_info \
            if self.market == 'stock' or self.market is None \
            else self.db.load_fund_info
        self.is_prepared = True
        return self.is_prepared

    @staticmethod
    def desc():
        return '  名称: 随机策略\n' + \
               '  说明: 随机选股测试策略\n' + \
               '  参数: market -- 选择品种(值为: fund 或 stock, 默认stock)'

    async def select(self):
        """
        根据策略，选择股票
        :return: [{code, name...}, {code, name}, ...]/None
        """
        codes = await self.db_load_func(projection=['code', 'name'])
        if codes is None:
            return None

        choice = random.sample(range(len(codes)), self.select_count)
        df = codes.iloc[choice]

        return df.reset_index(drop=True)

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        df = await self.db_load_func(filter={'code', code}, projection=['code', 'name'])
        return df
