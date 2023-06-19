from typing import List
import pandas as pd
import requests
import hiq_pyfetch as fetch
from datetime import datetime, timedelta

from tqdm import tqdm
from winq.data.winqdb import WinQDB


def stock_lhb(
    start_date: str = "2023-05-04", end_date: str = "2023-05-04"
) -> List:
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "SECURITY_CODE,TRADE_DATE",
        "sortTypes": "1,-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_DAILYBILLBOARD_DETAILS",
        "columns": "SECURITY_CODE,SECUCODE,SECURITY_NAME_ABBR,TRADE_DATE,EXPLAIN,CLOSE_PRICE,CHANGE_RATE,BILLBOARD_NET_AMT,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,BILLBOARD_DEAL_AMT,ACCUM_AMOUNT,DEAL_NET_RATIO,DEAL_AMOUNT_RATIO,TURNOVERRATE,FREE_MARKET_CAP,EXPLANATION,D1_CLOSE_ADJCHRATE,D2_CLOSE_ADJCHRATE,D5_CLOSE_ADJCHRATE,D10_CLOSE_ADJCHRATE",
        "source": "WEB",
        "client": "WEB",
        "filter": f"(TRADE_DATE<='{end_date}')(TRADE_DATE>='{start_date}')",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(data_json["result"]["data"])
    temp_df.reset_index(inplace=True)
    temp_df["index"] = temp_df.index + 1
    temp_df.columns = [
        "序号",
        "代码",
        "-",
        "名称",
        "-",
        "解读",
        "收盘价",
        "涨跌幅",
        "龙虎榜净买额",
        "龙虎榜买入额",
        "龙虎榜卖出额",
        "龙虎榜成交额",
        "市场总成交额",
        "净买额占总成交比",
        "成交额占总成交比",
        "换手率",
        "流通市值",
        "上榜原因",
        "-",
        "-",
        "-",
        "-",
    ]
    codes = list(temp_df['代码'].values)
    codes = [
        'sh' + code if code.startswith('6') else 'sz' + code for code in codes]

    return codes


def stock_15_25_min(
    code: str
) -> pd.DataFrame:
    symbol = code
    market = symbol[:2].lower()
    secid = '1.' + symbol[2:] if market == 'sh' else '0.' + symbol[2:]

    url = "https://push2.eastmoney.com/api/qt/stock/trends2/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "ndays": "1",
        "iscr": "1",
        "iscca": "0",
        "secid": secid,
        "_": "1623766962675",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    temp_df = pd.DataFrame(
        [item.split(",") for item in data_json["data"]["trends"]]
    )
    temp_df.columns = [
        "时间",
        "开盘",
        "收盘",
        "最高",
        "最低",
        "成交量",
        "成交额",
        "最新价",
    ]
    temp_df.index = pd.to_datetime(temp_df["时间"])
    date_format = temp_df.index[0].date().isoformat()
    temp_df = temp_df[
        date_format + " 09:15:00": date_format + " 09:26:00"
    ]
    temp_df.reset_index(drop=True, inplace=True)
    temp_df["open"] = pd.to_numeric(temp_df["开盘"])
    temp_df["close"] = pd.to_numeric(temp_df["收盘"])
    temp_df["high"] = pd.to_numeric(temp_df["最高"])
    temp_df["low"] = pd.to_numeric(temp_df["最低"])
    temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
    temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
    temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
    temp_df["time"] = pd.to_datetime(temp_df["时间"])
    temp_df["code"] = symbol

    return temp_df[['code', 'open', 'close', 'high', 'low', 'time']]


def stock_hot_rank() -> pd.DataFrame:
    url = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
    payload = {
        "appId": "appId01",
        "globalId": "786e4c21-70dc-435a-93bb-38",
        "marketType": "",
        "pageNo": 1,
        "pageSize": 100,
    }
    r = requests.post(url, json=payload)
    data_json = r.json()
    temp_rank_df = pd.DataFrame(data_json["data"])
    temp_rank_df.rename(columns={'sc': 'code', 'rk': 'rank'}, inplace=True)
    temp_rank_df['code'] = temp_rank_df['code'].apply(
        lambda code: code.lower())

    return temp_rank_df[['code', 'rank']]


async def hot_game(db, hot_codes=None):
    result = []
    pre_trade_date = None
    # pre_n_trade_date = None
    # n_days = 60
    # n_rise = 0.3
    trade_date = None
    hot_quot = {}

    if hot_codes is None:
        hot = stock_hot_rank()
        hot_codes = list(hot['code'].values)

    # 龙虎榜
    lhb_codes = []

    rank = 0
    proc_bar = tqdm(hot_codes)
    for code in proc_bar:
        rank += 1
        proc_bar.set_description('处理 {}'.format(code))
        df = stock_15_25_min(code)

        hot_quot[code] = df

        max = df['high'].max()
        last = df.iloc[-1]

        if last['high'] != max and last['close'] != max:
            continue

        if pre_trade_date is None:
            time = last['time']
            date = datetime.strptime(time.strftime('%Y%m%d'), '%Y%m%d')
            trade_date = date
            pre_trade_date = await fetch.fetch_prev_trade_date(trade_date)
            pre_trade_date = datetime.strptime(
                pre_trade_date.strftime('%Y%m%d'), '%Y%m%d')
            lhb_date = pre_trade_date.strftime('%Y-%m-%d')
            lhb_codes = stock_lhb(start_date=lhb_date, end_date=lhb_date)

            # pre_n_trade_date = pre_trade_date - timedelta(days=n_days)

        # 忽略龙虎榜
        if code in lhb_codes:
            continue

        # data = await db.load_stock_daily(filter={'trade_date': {'$gte': pre_n_trade_date, '$lte': pre_trade_date}, 'code': code},
        #                                  sort=[('trade_date', -1)])
        data = await db.load_stock_daily(filter={'trade_date': pre_trade_date, 'code': code},
                                         limit=1)
        if data is None or len(data) == 0:
            continue
        last_close, last_open, last_chg_pct = data.iloc[0][
            'close'], data.iloc[0]['open'], data.iloc[0]['chg_pct']

        # n_close = data.iloc[-1]['close']
        # nl_rise = (last_close - n_close)/n_close
        # if nl_rise >= n_rise:
        #     continue

        # if last_close < last_open:
        #     continue

        min = df['low'].min()
        chg_pct = (min - last_close) * 100 / last_close
        if chg_pct < -1.0:
            continue

        chg_pct = (last['close'] - last_close) * 100 / last_close

        # 涨幅小于1.0都抛弃它
        if chg_pct < 1.0 or chg_pct > 5.0:
            continue

        name = ''
        name_df = await db.load_stock_info(filter={'code': code}, projection=['code', 'name'])
        if name_df is not None and len(name_df) > 0:
            name = name_df.iloc[0]['name']
            if 'ST' in name.upper():
                continue

        industry = ''
        industry_df = await db.load_stock_industry_detail(filter={'stock_code': code}, projection=['name'])
        if industry_df is not None and len(industry_df) > 0:
            industry = ','.join(list(industry_df['name'].values))

        concept = ''
        concept_df = await db.load_stock_concept_detail(filter={'stock_code': code}, projection=['name'])
        if concept_df is not None and len(concept_df) > 0:
            concepts = list(concept_df['name'].values)
            concept = ''
            ignore_list = ['预盈预增', '预亏预减', '基金重仓', '标准普尔', 'MSCI中国', 'HS300_',
                           'AH股', '深证100R', '证金持股', 'QFII重仓', '深成500', '上证50_', '上证180_', '深股通', '沪股通', '富时罗素', '融资融券', '创业板综', '机构重仓', '高送转', '中字头', 'ST股']
            for i in range(len(concepts)):
                c = concepts[i]
                if '昨日' in c or c in ignore_list:
                    continue
                if (i + 1) % 5 == 0:
                    concept += '\n'
                if concept == '':
                    concept = c
                else:
                    concept += ',{}'.format(c)

        result.append(
            {'code': code, 'name': name, 'chg_pct': chg_pct, 'last_chg_pct': last_chg_pct, 'industry': industry, 'rank': rank, 'concept': concept})

    proc_bar.set_description('处理完成！')
    result = pd.DataFrame(result)
    if len(result) > 0:
        result = result.sort_values('chg_pct', ascending=False)
        result.reset_index(inplace=True, drop=True)
    return trade_date, hot_codes, hot_quot, result


if __name__ == '__main__':
    from winq import *

    # print(stock_lhb())

    # db = default(log_level='info')

    # async def test_hot_game():
    #     data = await hot_game(db, hot_codes=None)
    #     print(data)

    # print(datetime.now())
    # run_until_complete(test_hot_game())
    # print(datetime.now())
    
