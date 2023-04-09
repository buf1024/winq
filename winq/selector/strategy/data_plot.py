import os

import numpy as np
from winq.analyse.plot import my_plot, up_color
from winq.data.winqdb import WinQDB
from winq.selector.strategy.comm import normalize_date


class DataPlot:
    def __init__(self, db: WinQDB,
                 test_end_date=None,
                 load_daily=None, load_info=None,
                 fetch_daily=None, fetch_info=None) -> None:
        self.db = db

        self.test_end_date = normalize_date(test_end_date)

        self.load_daily = load_daily
        self.load_info = load_info
        self.fetch_daily = fetch_daily
        self.fetch_info = fetch_info

    async def plot(self, code, *,
                   limit=60, skip_test_end_date=True, s_data=None, marks=None):
        """
        plot图象观察
        :param s_data dict
        :param marks: [{color:xx, data:[{trade_date:.. tip:...}...]}]
        :param code:
        :param limit: k线数量
        :param skip_test_end_date
        :return:
        """
        data = await self.plot_data(code, limit, skip_test_end_date)
        if data is None or data.empty:
            return None

        if marks is None and self.test_end_date is not None:
            marks = []

        if self.test_end_date is not None:
            trade_date = self.test_end_date
            trade_date = await self.db.prev_trade_date(trade_date)

            tip = ''
            if s_data is not None:
                keys = ['1day', '3day', '5day', '10day', 'latest']
                for key, val in s_data.items():
                    if key in keys and val is not None and not np.isnan(val):
                        tmp_tip = '{} 涨幅:&nbsp;&nbsp;{}%'.format(key, val)
                        if len(tip) == 0:
                            tip = tmp_tip
                            continue
                        tip = '{}\n{}'.format(tip, tmp_tip)

            marks.append({'color': up_color(),
                          'symbol': 'diamond',
                          'data': [{'trade_date': trade_date,
                                    'tip': '留意后面涨幅\n{}'.format(tip)}]})

        return my_plot(data=data, marks=marks)

    async def plots(self, data, limit=60, skip_test_end_date=True):
        if data is None:
            return None

        charts = {}
        items = data.to_dict('records')
        for item in items:
            chart = await self.plot(code=item['code'], limit=limit, skip_test_end_date=skip_test_end_date, s_data=item)
            if chart is not None:
                charts[item['code']] = chart
        return charts

    async def plots_to_path(self, path, data, limit=60):
        charts = await self.plots(data=data, limit=limit)
        if charts is None:
            return
        os.makedirs(path, exist_ok=True)
        for k, v in charts.items():
            file_path = os.sep.join([path, k + '.html'])
            v.render(file_path)

    async def plot_data(self, code, limit, skip_test_end_date=True):
        flter = {'code': code}
        if self.test_end_date is not None and not skip_test_end_date:
            flter = {'code': code, 'trade_date': {'$lte': self.test_end_date}}

        data = await self.load_daily(filter=flter,
                                     limit=limit,
                                     sort=[('trade_date', -1)])
        
        data.sort_values('trade_date', inplace=True)
        data.reset_index(drop=True)

        return data
