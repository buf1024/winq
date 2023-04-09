from winq.common import get_datetime
import winq.log as log
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from tqdm import tqdm
import asyncio
from winq.selector.strategy.comm import normalize_date


class Strategy:
    def __init__(self, db, *,
                 test_end_date=None,
                 min_trade_days=60,
                 with_rt_kdata=True,
                 run_task_count=50,
                 load_daily=None,
                 load_info=None,
                 fetch_daily=None,
                 fetch_info=None,
                 min_hit_days=3
                 ):
        """
        :param db: winqdb
        :param test_end_date: 测试截止交易日，None为数据库中日期
        :param min_trade_days 最小交易天数
        """
        self.log = log.get_logger(self.__class__.__name__)
        self.db = db
        self.test_end_date = normalize_date(test_end_date)

        self.min_trade_days = min_trade_days

        self.with_rt_kdata = with_rt_kdata

        self.run_task_count = run_task_count

        self.load_daily = load_daily
        self.load_info = load_info
        self.fetch_daily = fetch_daily
        self.fetch_info = fetch_info

        self.min_hit_days = min_hit_days

        self.sort_by = None
        self.sort_reverse = True

        self.is_prepared = False

    @staticmethod
    def desc():
        pass

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """

        if kwargs is not None and 'test_end_date' in kwargs:
            self.test_end_date = normalize_date(kwargs['test_end_date'])
        if kwargs is not None and 'min_trade_days' in kwargs:
            self.min_trade_days = int(kwargs['min_trade_days'])

        if kwargs is not None and 'with_rt_kdata' in kwargs:
            self.with_rt_kdata = kwargs['with_rt_kdata']

        if kwargs is not None and 'run_task_count' in kwargs:
            self.run_task_count = kwargs['run_task_count']

        if kwargs is not None and 'min_hit_days' in kwargs:
            self.min_hit_days = kwargs['min_hit_days']

        if kwargs is not None and 'sort_by' in kwargs:
            self.sort_by = kwargs['sort_by']

        if kwargs is not None and 'sort_reverse' in kwargs:
            self.sort_reverse = kwargs['sort_reverse']

        return True

    async def destroy(self):
        """
        清理接口
        :return: True/False
        """
        return True

    async def select(self, codes=None, with_progress=True) -> Optional[pd.DataFrame]:
        """
        根据策略，选择股票
        :return: code, name 必须返回的, 1day, 3day, 5day, 10day, latest的涨幅，如果有尽量返回
            [{code, name...}, {code, name}, ...]/None
        """
        flter = None
        if codes is not None:
            flter = {'code': {'$in': codes}}
        codes = await self.load_info(
            filter=flter,
            projection=['code', 'name'])

        size = len(codes)
        if size == 0:
            return None

        s, r = int(size / self.run_task_count), size % self.run_task_count

        q = asyncio.Queue()
        asyncio.create_task(self._do_select_progress_task(
            q=q, size=size, with_progress=with_progress))

        tasks = []
        if s > 0:
            for i in range(s):
                start = i*self.run_task_count
                end = start + self.run_task_count if i != s - 1 else start+self.run_task_count + r
                tasks.append(self._do_select_task(q=q, codes=codes[start:end][:]))
        else:
            tasks.append(self._do_select_task(q=q, codes=codes[:]))

        rest = await asyncio.gather(*tasks)
        await q.join()
        rest = list(filter(lambda e: e is not None, rest))
        data = None
        if len(rest) > 0:
            data = pd.concat(rest).reset_index(drop=True)

        return data

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        """
        根据策略，测试股票是否符合策略
        :param code
        :param name
        :return: 符合策略的，返回和select 一样的结构
            code, name 必须返回的, 1day, 3day, 5day, 10day, latest的涨幅，如果有尽量返回
            [{code, name...}, {code, name}, ...]/None
        """
        raise Exception('选股策略 {} 没实现测试函数'.format(self.__class__.__name__))

    async def fit(self, code: str, name: str = None, with_stat=True, **kwargs) -> Optional[pd.DataFrame]:
        if not self.is_prepared and not await self.prepare(**kwargs):
            self.log.error('策略 {} 初始化失败'.format(self.__class__.__name__))
            return None

        data = await self.test(code=code, name=name)
        if data is None or data.empty:
            return data

        if with_stat:
            data = await self.stat_data(data=data)
        return data

    async def run(self, codes=None, **kwargs) -> Optional[pd.DataFrame]:
        if not self.is_prepared and not await self.prepare(**kwargs):
            self.log.error('策略 {} 初始化失败'.format(self.__class__.__name__))
            return None

        with_progress = kwargs['with_progress'] if 'with_progress' in kwargs else True

        data = await self.select(codes=codes, with_progress=with_progress)

        if data is None or data.empty:
            return data

        if self.sort_by is not None and self.sort_by in data.keys():
            data.sort_values(self.sort_by, inplace=True,
                             ascending=not self.sort_reverse)
            data.reset_index(inplace=True, drop=True)

        return data

    async def load_kdata(self, code, **kwargs):
        kdata = await self.load_daily(**kwargs)
        now = datetime.now().date()
        test_end_date = self.test_end_date.date()
        if kdata is not None and len(kdata) > 0 and \
                self.with_rt_kdata and test_end_date == now:
            data0 = kdata.iloc[0]
            db_latest = data0['trade_date'].date()
            if db_latest != now:
                start_date = db_latest + timedelta(days=1)
                end_date = now
                if end_date >= start_date and \
                        self.fetch_daily is not None:
                    new_data = await self.fetch_daily(code=code, name=data0['name'], skip_rt=False, start=start_date, end=end_date)
                    bars = new_data['bars']
                    if bars is not None and len(bars) > 0:
                        kdata = pd.concat([bars, kdata])
                        kdata.reset_index(drop=True)

        return kdata

   

    async def _do_select_task(self, q: asyncio.Queue, codes: pd.DataFrame) -> Optional[pd.DataFrame]:
        return await self.do_select(q=q, codes=codes)

    async def _do_select_progress_task(self, q: asyncio.Queue, size: int, with_progress=True):
        proc_bar = tqdm(range(size)) if with_progress else range(size)
        for _ in proc_bar:
            code, name = await q.get()
            if with_progress:
                proc_bar.set_description('处理 {}({})'.format(code, name))
            q.task_done()

        if with_progress:
            proc_bar.set_description('处理完成')
            proc_bar.close()

    async def do_select(self, q: asyncio.Queue,
                        codes: pd.DataFrame) -> Optional[pd.DataFrame]:
        if codes is None:
            return None
        select = []
        for item in codes.to_dict('records'):
            await q.put((item['code'], item['name']))
            got_data = await self.test(code=item['code'], name=item['name'])
            if got_data is not None:
                select = select + got_data.to_dict('records')

        df = None
        if len(select) > 0:
            df = pd.DataFrame(select)

        return df

    async def result_with_data(self, code, hit_date, with_now_date=True) -> Optional[pd.DataFrame]:
        hit_date = get_datetime(hit_date)

        flter = {'code': code} if with_now_date else {'code': code,
                                                      'trade_date': {'$lte': self.test_end_date, }}
        kdata = await self.load_kdata(code=code,
                                      filter=flter,
                                      limit=self.min_trade_days,
                                      sort=[('trade_date', -1)])

        if kdata is None or kdata.shape[0] == 0:
            return None

        hit_index = list(kdata[kdata['trade_date'] == hit_date].index)
        if len(hit_index) != 1:
            return None

        hit_index = hit_index[0]
        hit_data = kdata.iloc[hit_index]

        n_data = []
        for index in range(hit_index, -1, -1):
            data = kdata.iloc[index]
            t = {}
            t['trade_date'] = data['trade_date']
            t['hit_chg_pct'] = round(
                (data['close'] - hit_data['close']) * 100.0 / hit_data['close'], 2)

            n_data.append(t)

        n_data = pd.DataFrame(n_data)

        kdata = pd.merge(kdata, n_data, how='left', on='trade_date')
        kdata.fillna(0, inplace=True)
        return kdata

    @staticmethod
    def is_long_leg(df, ratio, side=None) -> bool:
        close, high, low, open_ = df['close'], df['high'], df['low'], df['open']
        if high == low:
            return False
        if side is None:
            side = ['top', 'bottom']
        if isinstance(side, str):
            side = [side]
        for s in side:
            if s == 'top':
                r = (high - close) * 100 / (high - low)
                if r > ratio:
                    return True
            if s == 'bottom':
                r = (open_ - low) * 100 / (high - low)
                if r > ratio:
                    return True

        return False

    @staticmethod
    def is_short_leg(df, ratio, side=None) -> bool:
        return not Strategy.is_long_leg(df=df, ratio=ratio, side=side)

    @staticmethod
    def falling_down(df) -> float:
        last_close, high, chg_pct = df['close'] / \
            (1.0 + df['chg_pct']/100), df['high'],  df['chg_pct']

        high_chg_pct = (high - last_close) * 100 / last_close

        return high_chg_pct - chg_pct
    
    @staticmethod
    def is_red(df) -> bool:
        return df['close'] >= df['open']
    
    @staticmethod
    def is_20cm(code) -> bool:
        prefix = code[:4]
        return prefix == 'sh68' or prefix == 'sz30'
    
    @staticmethod
    def cal_gaps(kdata: pd.DataFrame) -> pd.DataFrame:
        """kdata 按时间降序排序

        Args:
            kdata (pd.DataFrame): 降序

        Returns:
            pd.DataFrame: 
        """
        kdata['gap_pct'] = 0.0
        kdata['is_gap_fill'] = False
        
        prev_low, prev_high = 0, 0
        max, min = -99999999.0, 99999999.0
        for i in range(len(kdata)):
            data = kdata.iloc[i]
            low, high = data['low'], data['high']
            
            if prev_high != 0 and prev_low != 0:
                 # 下跌缺口
                if prev_high < low:
                    # 以low为基准，计算缺口百分比
                    gap_pct = round((prev_high - low) / low *100, 2)
                    kdata.loc[i, 'gap_pct'] = gap_pct
                    if max >= low:
                        kdata.loc[i, 'is_gap_fill'] = True
                
                # 上升缺口
                if prev_low > high:
                    # 以high为基准，计算缺口百分比
                    gap_pct = round((prev_low - high) / high *100, 2)
                    kdata.loc[i, 'gap_pct'] = gap_pct
                    if min <= high:
                        kdata.loc[i, 'is_gap_fill'] = True
                        
            prev_low, prev_high = low, high
            if low < min:
                min = low
            if high > max:
                max = high
                
        return kdata