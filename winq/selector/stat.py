import hiq_pyfetch as fetch
import pandas as pd
from datetime import datetime


async def stat(path, db, stat_days):
    data = pd.read_excel(path, sheet_name='实盘', engine='openpyxl')
    data = data[~data['代码'].isnull()]
    data.reset_index(inplace=True)

    # data_win = data[data['盈利'] > 0]
    # data_lost = data[data['盈利'] <= 0]

    # total, win, lost = len(data), len(data_win), len(data_lost)

    # d = {'交易总数': total, '盈利总数': win, '亏损总数': lost, '胜率': round(win * 100.0 / total, 2) }

    stat_data = []
    for index in range(len(data)):
        item = data.iloc[index]
        code, date, price = item['代码'], item['买入时间'], item['买入价']

        s_data = {'代码': code, '名称': item['名称'], '买入时间': date, '买入价': price}
        for i in range(stat_days):
            s_data['n_{}'.format(i+1)] = None

        for i in range(stat_days):
            trade_date = await fetch.fetch_next_trade_date(date)
            trade_date = datetime.strptime(
                trade_date.strftime('%Y%m%d'), '%Y%m%d')
            next_item = await db.load_stock_daily(filter={'code': code,
                                                          'trade_date': trade_date},
                                                  limit=1)
            if next_item is None or len(next_item) <= 0:
                break

            close = next_item.iloc[0]['close']
            rise = round((close - price) * 100.0 / price, 2)

            s_data['n_{}'.format(i+1)] = rise

            date = next_item.iloc[0]['trade_date']

        stat_data.append(s_data)

    stat_data = pd.DataFrame(stat_data)

    w_stat = {}
    for i in range(stat_days):
        w_stat['n_{}'.format(i+1)] = None

    for i in range(stat_days):
        key = 'n_{}'.format(i+1)
        total = stat_data[~stat_data[key].isnull()]
        win = total[total[key] > 0]
            
        total, win = len(total), len(win)
        
        if total == 0:
            continue
            
        rate = round(win*100.0/total, 2)
        w_stat[key] = '{}%({}/{})'.format(rate, win, total)
        
    w_stat = pd.DataFrame([w_stat])
        
    return stat_data, w_stat

if __name__ == '__main__':
    from winq import *

    async def test_stat():
        db = default(log_level='info')
        path = '/Users/luoguochun/Downloads/stock/交易记录.xlsx'

        s, w = await stat(path=path, db=db, stat_days=5)
        print(s)

    run_until_complete(test_stat())
