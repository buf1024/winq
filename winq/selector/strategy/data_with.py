from datetime import datetime, timedelta
import pandas as pd
from winq.data.winqdb import WinQDB
from winq.selector.strategy.comm import normalize_date


class DataWith:
    def __init__(self, db: WinQDB,
                 load_daily=None, load_info=None,
                 fetch_daily=None, fetch_info=None) -> None:
        self.db = db
        self.load_daily = load_daily
        self.load_info = load_info
        self.fetch_daily = fetch_daily
        self.fetch_info = fetch_info

    async def data_with(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        pass


class DataWithStat(DataWith):
    def __init__(self, db: WinQDB,
                 test_end_date=None,
                 load_daily=None, load_info=None, fetch_daily=None,
                 fetch_info=None) -> None:
        super().__init__(db, load_daily, load_info, fetch_daily, fetch_info)
        self.test_end_date = normalize_date(test_end_date)

    async def data_with(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        now = datetime.now()
        now = datetime(year=now.year, month=now.month, day=now.day)

        rise_dict = {'1day': None, '2day': None, '3day': None,
                     '4day': None, '5day': None, '10day': None, 'now': None}
        trade_days = 0
        test_date = self.test_end_date + timedelta(days=1)
        while test_date <= now:
            if await self.db.is_trade_date(test_date):
                trade_days = trade_days + 1
                if trade_days == 1:
                    rise_dict['1day'] = test_date
                if trade_days == 2:
                    rise_dict['2day'] = test_date
                if trade_days == 3:
                    rise_dict['3day'] = test_date
                if trade_days == 4:
                    rise_dict['4day'] = test_date
                if trade_days == 5:
                    rise_dict['5day'] = test_date
                if trade_days == 10:
                    rise_dict['10day'] = test_date
            test_date = test_date + timedelta(days=1)
        rise_dict['latest'] = now

        info_list = []

        for item in data.to_dict('records'):
            info_dict = {'code': item['code']}
            for key, val in rise_dict.items():
                if val is not None:
                    kdata = await self.load_daily(filter={'code': item['code'],
                                                          'trade_date': {'$gte': self.test_end_date, '$lte': val}},
                                                  sort=[('trade_date', -1)])
                    if kdata is None:
                        # 可能停牌
                        info_dict[key] = 0
                    else:
                        if len(kdata) < 2:
                            info_dict[key] = 0
                        else:
                            close, pre_close = kdata.iloc[0]['close'], kdata.iloc[-1]['close']
                            rise = round((close - pre_close)
                                         * 100 / pre_close, 2)
                            info_dict[key] = rise

            info_list.append(info_dict)

        add_df = pd.DataFrame(info_list)
        data = data.merge(add_df, on='code')
        return data


class DataWithConcept(DataWith):
    def __init__(self, db: WinQDB,
                 load_daily=None, load_info=None, fetch_daily=None,
                 fetch_info=None) -> None:
        super().__init__(db, load_daily, load_info, fetch_daily, fetch_info)

    async def data_with(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        info_list = []
        for item in data.to_dict('records'):
            info_dict = {'code': item['code']}
            info_dict['concept'] = await self.stock_concept_str(code=item['code'])
            info_list.append(info_dict)

        add_df = pd.DataFrame(info_list)
        return data.merge(add_df, on='code')

    async def stock_concept_detail(self, code):
        return await self.db.load_stock_concept_detail(filter={'stock_code': code})

    async def stock_concept_str(self, code):
        df = await self.stock_concept_detail(code=code)
        if df is not None and not df.empty:
            s = []
            lst = list(df['name'])
            while len(lst) > 0:
                tmp_lst = lst[:3]
                s.append(','.join(tmp_lst))

                lst = lst[3:]
            return '\n'.join(s)
        return ''
