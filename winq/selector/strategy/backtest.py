from ast import Dict
import asyncio
from typing import Optional
from tqdm import tqdm
from winq.selector.strategy.comm import normalize_date


class Backtest:
    def __init__(self, strategy, test_times: int, test_step: int = 1,
                 test_end_date=None, ) -> None:
        self.strategy = strategy
        self.test_times = test_times
        self.test_step = test_step
        self.test_end_date = normalize_date(test_end_date)
        self.db = self.strategy.db

    async def _run_task(self, q: asyncio.Queue, test_date, **kwargs):
        
        data = await self.strategy.run(**kwargs)

        await q.put(test_date)

        return None if data is None else {test_date: data}

    async def _do_select_progress_task(self, q: asyncio.Queue, size: int):
        proc_bar = tqdm(range(size))
        proc_bar.set_description('开始并发处理，耐心等待')
        for _ in proc_bar:
            test_date = await q.get()
            proc_bar.set_description('处理 {} 完成'.format(test_date))
            q.task_done()

        proc_bar.set_description('处理完成')
        proc_bar.close()

    async def run(self, **kwargs) -> Optional[Dict]:
        test_end_date = self.test_end_date
        while not await self.db.is_trade_date(test_end_date):
            test_end_date = await self.db.prev_trade_date(test_end_date)

        q = asyncio.Queue()
        tasks = []

        kwargs['with_progress'] = False
        for _ in range(self.test_times):
            test_date = test_end_date.strftime('%Y-%m-%d')
            kwargs['test_end_date'] = test_date
            if not await self.strategy.prepare(**kwargs):
                raise Exception('prepare backtest error!')

            tasks.append(self._run_task(q=q, test_date=test_date, **kwargs))

            test_end_date = await self.db.prev_trade_date(
                test_end_date, self.test_step)

        asyncio.create_task(self._do_select_progress_task(
            q=q, size=self.test_times))

        rest = await asyncio.gather(*tasks)
        await q.join()
        
        result = {}
        
        rest = list(filter(lambda e: e is not None, rest))
        for r in rest:
            result.update(r)

        return result
