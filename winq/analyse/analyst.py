from functools import partial
from typing import Optional
# from winq.common import is_stock
from winq.data import WinQDB
import akshare as ak
import pandas as pd


class Analyst:
    def __init__(self, db: WinQDB) -> None:
        self.db = db

    async def code_name(self, code, name=None):
        if name is not None:
            return name

        load_info_func = self.db.info_func(code)

        name_df = await load_info_func(filter={'code': code}, limit=1)
        if name_df is not None and not name_df.empty:
            name = name_df.iloc[0]['name']
        return name

    async def stock_concept(self, code) -> Optional[str]:

        # if not is_stock(code):
        #     return None

        # func = partial(self.db.load_stock_concept_detail,
        #                filter={'stock_code': code})
        # return await self.stock_multi_str(func=func)
        pass

    async def stock_industry(self, code) -> Optional[str]:
        # if not is_stock(code):
        #     return None

        # func = partial(self.db.load_stock_industry_detail,
        #                filter={'stock_code': code})
        # return await self.stock_multi_str(func=func)
        pass

    async def stock_multi_str(self, func):
        df = await func()
        if df is not None and not df.empty:
            s = []
            lst = list(df['name'])
            while len(lst) > 0:
                tmp_lst = lst[:3]
                s.append(','.join(tmp_lst))

                lst = lst[3:]
            return '\n'.join(s)
        return ''

    async def stock_relate(self, code: str = None, name: str = None) -> Optional[pd.DataFrame]:
        if code is None and name is None:
            return None

        name_df = None
        cond = {'code': code} if code is not None else {'name': name}

        name_df = await self.db.load_stock_info(filter=cond, projection=['code', 'name'])
        if name_df is None or name_df.empty:
            return None

        code, name = name_df.iloc[0]['code'], name_df.iloc[0]['name'],

        df = ak.stock_hot_rank_relate_em(code)
        if df is None or df.empty:
            return None

        df.rename(columns={'时间': 'date', '股票代码': 'code',
                  '相关股票代码': 'relate_code', '涨跌幅': 'rise'}, inplace=True)
        df['code'] = df['code'].str.lower()
        df['relate_code'] = df['relate_code'].str.lower()

        name_df['concept'] = await self.stock_concept(code)
        name_df['industry'] = await self.stock_industry(code)

        df = df.merge(name_df, on=['code'], how='left')
        if name_df is None or name_df.empty:
            return None

        name_df = await self.db.load_stock_info(filter={'code': {'$in': list(df['relate_code'].values)}}, projection=['code', 'name'])
        if name_df is None or name_df.empty:
            return None

        concept = [await self.stock_concept(x) for x in name_df['code'].values]
        industry = [await self.stock_industry(x) for x in name_df['code'].values]

        name_df['relate_concept'] = concept
        name_df['relate_industry'] = industry

        name_df.rename(columns={'code': 'relate_code',
                       'name': 'relate_name'}, inplace=True)

        return df.merge(name_df, on=['relate_code'], how='left')


if __name__ == '__main__':
    from winq.common import run_until_complete

    async def stock_industry(a):
        # df = await a.stock_industry(code='sz000546')
        df = await a.stock_relate('sz000546')
        print(df)

    db = WinQDB(uri='mongodb://localhost:27017/')
    db.init()
    a = Analyst(db=db)
    run_until_complete(
        stock_industry(a)
    )
