"""
赢家股票投资分析工具
"""

__version__ = "0.0.1"
__author__ = "450171094@qq.com"

from winq.log import *
from winq.common import *
from winq.retry import *

from winq.config import *
from winq.data import *
from winq.analyse import *
from hisql import *
import pandas as pd


def default(log_level='debug'):
    _, conf_dict = init_def_config()
    conf_dict['log']['level'] = log_level
    setup_log(conf_dict, 'winq.log', True)

    return setup_db(conf_dict, WinQDB)


async def winq_plot(db, code, limit, start=None, end=None):
    """
    @param db:
    @param code:
    @param limit:
    @param start:
    @param end:
    """
    from datetime import datetime
    if start is None:
        start = datetime(year=1990, month=1, day=1)
    if end is None:
        now = datetime.now()
        end = datetime(year=now.year, month=now.month, day=now.day)

    df = None

    load_daily_func = db.daily_func(code)

    df = await load_daily_func(filter={'code': code, 'trade_date': {'$gte': start, '$lte': end}}, limit=limit, sort=[('trade_date', -1)])

    if df is None:
        return None

    df = df.sort_values(by='trade_date', ascending=True)
    return my_plot(df)


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
