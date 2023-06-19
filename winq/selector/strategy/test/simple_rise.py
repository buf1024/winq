from typing import Optional
from datetime import datetime, timedelta
import pandas as pd
import hiq_pyfetch as fetch
from winq.selector.strategy.strategy import Strategy


class SimpleRise(Strategy):
    """
    配合SuperFalling使用，用于实盘选
    
    热门超跌上涨。
    实战：10:30 20cm涨幅不高于5%，回撤不大于2%，10cm涨幅不超过3%，回撤不大于1%，且当前不是最低，缓慢上涨。
    选股：当日下跌，高点应该5日内出现，高点到当日，回撤2~10个点，最高点前连续上涨（红盘或大于-0.5%），累计上涨大于10%
    如2023/03/29 688313。
    前一晚，应该选择符合条件的票，10:20计算进行实盘交易。
    """

    def __init__(self, db, **kwargs):
        super().__init__(db, **kwargs)
        self.max_down_as_rise = -0.5
        self.cal_time = None
        self.max_turnover = 7.0
        self.max_20cm_rise = 5.0
        self.max_20cm_falling = 2.0
        self.max_10cm_rise = 3.0
        self.max_10cm_falling = 1.0

    @staticmethod
    def desc():
        return '  名称: 热股超跌实盘交易(基于日线)\n' + \
               '  说明: 配合SuperFalling使用，用于实盘选\n' + \
               '  参数: max_down_as_rise -- 下跌多少视为上涨(默认: -0.5)\n' + \
               '        cal_time -- 计算截止时间(默认None)\n' + \
               '        max_turnover -- 实盘交易时，最大换手率(默认: 7.0)\n' + \
               '        max_20cm_rise -- 实盘交易时，20cm股票最大涨幅(默认: 5.0)\n' + \
               '        max_20cm_falling -- 实盘交易时，20cm股票最大回跌(默认: 2.0)\n' + \
               '        max_10cm_rise -- 实盘交易时，10cm股票最大涨幅(默认: 3.0)\n' + \
               '        max_10cm_falling -- 实盘交易时，10cm股票最大回跌(默认: 1.0)'

    async def prepare(self, **kwargs):
        """
        初始化接口
        :param kwargs:
        :return: True/False
        """
        await super().prepare(**kwargs)
        try:
            if kwargs is not None and 'max_down_as_rise' in kwargs:
                self.max_down_as_rise = float(kwargs['max_down_as_rise'])
            
            if kwargs is not None and 'cal_time' in kwargs:
                self.cal_time = kwargs['cal_time']
            if kwargs is not None and 'max_turnover' in kwargs:
                self.max_turnover = float(kwargs['max_turnover'])
            if kwargs is not None and 'max_20cm_rise' in kwargs:
                self.max_20cm_rise = float(kwargs['max_20cm_rise'])
            if kwargs is not None and 'max_20cm_falling' in kwargs:
                self.max_20cm_falling = float(kwargs['max_20cm_falling'])
            if kwargs is not None and 'max_10cm_rise' in kwargs:
                self.max_10cm_rise = float(kwargs['max_10cm_rise'])
            if kwargs is not None and 'max_10cm_falling' in kwargs:
                self.max_10cm_falling = float(kwargs['max_10cm_falling'])

        except ValueError:
            self.log.error('策略参数不合法')
            return False
        self.is_prepared = True
        return self.is_prepared
    
    async def test_simple_rise(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        new_data = None
        try:
            new_data = await self.fetch_daily(code=code, name=name, freq=15, start=self.test_end_date, end=self.test_end_date)
        except Exception as e:
            print('remote request exception, code={}, name={}'.format(code, name))
            return None
        kdata = None
        new_data = new_data['bars']
        if new_data is not None and len(new_data) > 0:
            kdata = new_data.sort_values(
                'trade_date', ascending=False).reset_index(drop=True)
    
            if self.cal_time is not None:
                test_date_str = self.test_end_date.strftime('%Y%m%d')
                s = datetime.strptime('{} {}'.format(test_date_str, self.cal_time), '%Y%m%d %H:%M:%S')
                kdata = kdata[kdata['trade_date'] <= s]

        if kdata is None:
            return None
        
        chg_pct = kdata['chg_pct'].sum()
        if chg_pct < 1.5 or chg_pct > 7.0:
            return None
        
        # data = kdata[kdata['chg_pct'] < 0.0]
        # if len(data) > 0:
        #     return None
        
        # latest = await self.load_daily(filter={'code': code,
        #                                        'trade_date': {'$lt': self.test_end_date}},
        #                                limit=1,
        #                                sort=[('trade_date', -1)])
        # if latest is None:
        #     return None
        
        # last_close = latest.iloc[0]['close']
        
        # last_high = -1
        # for i in range(len(kdata)):
        #     df = kdata.iloc[i]
        #     high, close, low, chg_pct_5m = df['high'], df['close'], df['low'], df['chg_pct']
            
        #     if chg_pct_5m < -1.0:
        #         return None
            
            # low_chg_pct = round((low - last_close) / last_close * 100, 2)
            # high_chg_pct = round((high - last_close) / last_close * 100, 2)
            # chg_pct = round((close - last_close) / last_close * 100, 2)

            # if chg_pct < -1.0:
            #     return None
            
            # if last_high <= 0:
            #     last_high = high
            #     continue
            
            # if last_high < close:
            #     break;
            
            # last_high = high
        # chg_pct_first = round((kdata.iloc[0]['close'] - last_close) / last_close * 100, 2)
        # chg_pct_last = round((kdata.iloc[-1]['close'] - last_close) / last_close * 100, 2)

        # if chg_pct_last >= chg_pct_first:
        #     return None

        got_data = dict(code=code, name=name)
        
        return pd.DataFrame([got_data])
        
        

    async def test(self, code: str, name: str = None) -> Optional[pd.DataFrame]:
        return await self.test_simple_rise(code=code, name=name)
        # new_data = None
        # try:
        #     new_data = await self.fetch_daily(code=code, name=name, freq=5, start=self.test_end_date, end=self.test_end_date)
        # except Exception as e:
        #     print('remote request exception, code={}, name={}'.format(code, name))
        #     return None
        # kdata = None
        # new_data = new_data['bars']
        # if new_data is not None and len(new_data) > 0:
        #     kdata = new_data.sort_values(
        #         'trade_date', ascending=False).reset_index(drop=True)
    
        #     if self.cal_time is not None:
        #         test_date_str = self.test_end_date.strftime('%Y%m%d')
        #         s = datetime.strptime('{} {}'.format(test_date_str, self.cal_time), '%Y%m%d %H:%M:%S')
        #         kdata = kdata[kdata['trade_date'] <= s]
        #     # 第一个5分钟是不要的
        #     kdata = kdata[0:-1]

        # if kdata is None:
        #     return None
        
        # turnover = kdata['turnover'].sum()
        # if turnover > self.max_turnover:
        #     return None

        # latest = await self.load_daily(filter={'code': code,
        #                                        'trade_date': {'$lt': self.test_end_date}},
        #                                limit=1,
        #                                sort=[('trade_date', -1)])
        # if latest is None:
        #     return None

        # is_20cm_code = self.is_20cm(code)
        # max_rise = self.max_20cm_rise if is_20cm_code else self.max_10cm_rise
        # max_falling = self.max_20cm_falling if is_20cm_code else self.max_10cm_falling

        # last_close = latest.iloc[0]['close']

        # the_high, the_low, the_close = kdata.iloc[0]['high'], kdata.iloc[0]['low'],kdata.iloc[0]['close']

        # for i in range(len(kdata)):
        #     df = kdata.iloc[i]
        #     high, close, low = df['high'], df['close'], df['low']

        #     the_high = high if high > the_high else the_high
        #     the_low = low if low < the_low else the_low

        #     low_chg_pct = round((low - last_close) / last_close * 100, 2)
        #     high_chg_pct = round((high - last_close) / last_close * 100, 2)
        #     chg_pct = round((close - last_close) / last_close * 100, 2)

        #     if low_chg_pct < self.max_down_as_rise or low_chg_pct > max_rise or \
        #             high_chg_pct < self.max_down_as_rise or high_chg_pct > max_rise or \
        #             chg_pct < self.max_down_as_rise or chg_pct > max_rise:
        #         return None

        # high_chg_pct = round((the_high - last_close) / last_close * 100, 2)
        # chg_pct = round((the_close - last_close) / last_close * 100, 2)

        # falling = high_chg_pct - chg_pct
        # if falling > max_falling:
        #     return None

        # low_chg_pct = round((the_low - last_close) / last_close * 100, 2)

        # got_data = dict(code=code, name=name,
        #                 falling_chg_pct=falling,
        #                 chg_pct=chg_pct,
        #                 low_chg_pct=low_chg_pct,
        #                 high_chg_pct=high_chg_pct)
        # return pd.DataFrame([got_data])

if __name__ == '__main__':
    from winq import *
    
    db = default(log_level='debug')

    async def test_trading():
        s = SimpleRise(db=db,
                         test_end_date='20230417',
                         with_rt_kdata=True,
                         run_task_count=50,
                         load_daily=db.load_stock_daily,
                         load_info=db.load_stock_info,
                         fetch_daily=fetch.fetch_stock_bar
                         )

        await s.prepare(cal_time='10:30:00',
                        max_down_as_rise=-1.0,
                        max_turnover=7.0,
                        max_20cm_rise=5.0,
                        max_20cm_falling=2.0,
                        max_10cm_rise=3.0,
                        max_10cm_falling=1.5,
                        sort_by='chg_pct')

        data = await s.test(code='sh688121')
        print(data)

    run_until_complete(test_trading())
