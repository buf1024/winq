from typing import Optional
import pandas as pd
from winq.selector.strategy.strategy import Strategy


class EnlargeTrade(Strategy):
    """
    不准
    """

    def __init__(self, db, **kwargs):
        super().__init__(db, **kwargs)
        self.min_hit_days = 3
        self.min_chg = 0.0
        self.min_amt_chg = 0.0
        self.min_vol_chg = 0.0

    @staticmethod
    def desc():
        return '  名称: 城建量持续放大(基于日线)\n' + \
               '  说明: 持续放大\n' + \
               '  参数: min_hit_days -- 条件至少命中天数\n' + \
               '        min_chg -- 至少上涨\n' + \
               '        min_amt_chg -- 成交额变化\n' + \
               '        min_vol_chg -- 成交量变化'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'min_hit_days' in kwargs:
                self.min_hit_days = int(kwargs['min_hit_days'])
            if kwargs is not None and 'min_chg' in kwargs:
                self.min_chg = float(kwargs['min_chg'])
            if kwargs is not None and 'min_amt_chg' in kwargs:
                self.min_amt_chg = float(kwargs['min_amt_chg'])
            if kwargs is not None and 'min_vol_chg' in kwargs:
                self.min_vol_chg = float(kwargs['min_vol_chg'])

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

        hit_days = 0
        for i in range(len(kdata)):
            df = kdata.iloc[i]
            chg_pct, amt_chg_pct, vol_chg_pct = df['chg_pct'], df['amount_chg_pct'], df['volume_chg_pct']
            if chg_pct >= self.min_chg and \
                    amt_chg_pct >= self.min_amt_chg and \
                vol_chg_pct >= self.min_vol_chg:
                hit_days = hit_days + 1
                continue
            break

        if hit_days < self.min_hit_days:
            return None

        data0 = kdata.iloc[0]
        min_hit_close, n_close = kdata.iloc[self.min_hit_days]['close'], data0['close']
        min_hit_up = round((n_close - min_hit_close) * 100 / min_hit_close, 2)

        hit_data = kdata.iloc[hit_days-1]
        hit_close = hit_data['close'] / (1+hit_data['chg_pct']/100.0)
        hit_chg_pct = round(
            (data0['close'] - hit_close)*100 / hit_close, 2)
        got_data = dict(code=code, name=name,
                        min_hit_close=min_hit_close, close=n_close, min_hit_chg_pct=min_hit_up,
                        hit_start=hit_data['trade_date'],
                        hit_chg_pct=hit_chg_pct,
                        hit_days=hit_days)
        return pd.DataFrame([got_data])


if __name__ == '__main__':
    from winq import *

    db = default(log_level='error')
    s = EnlargeTrade(db=db, test_end_date='20220106')

    async def test():
        await s.prepare()
        df = await s.test('sz002432')
        print(df)

    run_until_complete(test())
