from winq.data.mongodb import MongoDB
from typing import Optional
import pandas as pd
from datetime import datetime


class WinQDB(MongoDB):
    def __init__(self, uri='mongodb://localhost:27017/', pool=5, db='hiq'):
        super().__init__(uri, pool, db)

        self.meta = {
            # 股票
            # 股票信息
            'stock_info': {'code': '代码', 'name': '名称', 'listing_date': '上市日期',
                           'block': '板块', 'is_margin': '是否融资融券标的'},
            # 股票日线数据
            'stock_daily': {'code': '代码', 'name': '名称', 'trade_date': '交易日',
                            'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                            'volume': '成交量(股)', 'amount': '成交额(元)',
                            'turnover': '换手率', 'hfq_factor': '后复权因子', 'chg_pct': '涨跌比(百分比)',
                            'volume_chg_pct': '成交量变更(百分比)', 'amount_chg_pct': '成交额变更(百分比)'},
            # 股票指标
            'stock_index': {'code': '代码', 'name': '名称', 'trade_date': '交易日', 'price': '股价',
                            'pe': '市盈率', 'pb': '市净率', 'total_mv': '总市值', 'currency_value': '流通市值'},

            # 行业信息
            'stock_industry': {
                'code': '行业代码', 'name': '行业名称'},
            # 行业详情
            'stock_industry_detail': {'code': '行业代码', 'name': '行业名称',
                                      'stock_code': '股票代码', 'stock_name': '股票名称'},
            # 行业日线
            'stock_industry_daily': {'code': '代码', 'name': '名称', 'trade_date': '交易日',
                                     'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                                     'volume': '成交量(股)', 'amount': '成交额(元)',
                                     'turnover': '换手率', 'hfq_factor': '后复权因子', 'chg_pct': '涨跌比(百分比)',
                                     'volume_chg_pct': '成交量变更(百分比)', 'amount_chg_pct': '成交额变更(百分比)'},

            # 概念信息
            'stock_concept': {
                'code': '概念代码', 'name': '概念名称'},
            # 同花顺概念详情
            'stock_concept_detail': {'code': '概念代码', 'name': '概念名称',
                                     'stock_code': '股票代码', 'stock_name': '股票名称'},
            # 概念日线
            'stock_concept_daily': {'code': '代码', 'name': '名称', 'trade_date': '交易日',
                                    'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                                    'volume': '成交量(股)', 'amount': '成交额(元)',
                                    'turnover': '换手率', 'hfq_factor': '后复权因子', 'chg_pct': '涨跌比(百分比)',
                                    'volume_chg_pct': '成交量变更(百分比)', 'amount_chg_pct': '成交额变更(百分比)'},

            # 业绩表
            'stock_yjbb': {'year': '年份', 'season': '季度，1~4', 'season_date': '季度时间',
                           'code': '代码', 'name': '名称', 'mgsy': '每股收益',
                           'yysr': '(营业收入-营业收入)', 'yysr_tbzz': '(营业收入-同比增长)', 'yysr_jdhbzz': '(营业收入-季度环比增长)',
                           'jlr': '(净利润-净利润)', 'jlr_tbzz': '(净利润-同比增长)', 'jlr_jdhbzz': '(净利润-季度环比增长)',
                           'mgzc': '(每股净资产)', 'jzcsyl': '(净资产收益率)', 'mgjyxjl': '(每股经营现金流量)',
                           'xslll': '(销售毛利率)'
                           },

            # 融资融券明细数据
            'stock_margin': {'code': '代码', 'name': '名称', 'trade_date': '交易日',
                             'close': '收盘价(元)', 'chg_pct': '涨跌幅(%)',
                             'rz_ye': '融资余额(元)(RZYE)', 'rz_ye_zb': '融资余额占流通市值比(%)(RZYEZB)', 'rz_mre': '融资买入额(元)',
                             'rz_che': '融资偿还额(元)', 'rz_jme': '融资净买入(元)',
                             'rq_ye': '融券余额(元)', 'rq_yl': '融券余量(股)', 'rq_mcl': '融券卖出量(股)', 'rq_chl': '融券偿还量(股)',
                             'rq_jmg': '净卖出(股)',
                             'rz_rq_ye': '融资融券余额(元)', 'rz_rq_ye_cz': '融资融券余额差值(元)'},

            # 指数信息
            'index_info': {'code': '代码', 'name': '名称', 'listing_date': '上市日期',
                           'block': '板块', 'is_margin': '是否融资融券标的'},
            # 指数日线数据, 如: index_daily
            'index_daily': {'code': '代码', 'name': '名称', 'trade_date': '交易日',
                            'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                            'volume': '成交量(股)', 'amount': '成交额(元)',
                            'turnover': '换手率', 'hfq_factor': '后复权因子', 'chg_pct': '涨跌比(百分比)',
                            'volume_chg_pct': '成交量变更(百分比)', 'amount_chg_pct': '成交额变更(百分比)'},

            # 基金
            # 基金信息
            'fund_info': {
                'code': '基金代码', 'name': '基金简称'
            },
            # 基金净值信息
            'fund_net': {
                'code': '基金代码', 'name': '基金简称', 'trade_date': '交易日', 'net': '净值', 'net_acc': '累计净值',
                'chg_pct': '日增长率', 'apply_status': '申购状态', 'redeem_status': '赎回状态'
            },
            # 场内基金日线数据
            'fund_daily': {'code': '代码', 'name': '名称', 'trade_date': '交易日',
                           'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                           'volume': '成交量(股)', 'amount': '成交额(元)',
                           'turnover': '换手率', 'hfq_factor': '后复权因子', 'chg_pct': '涨跌比(百分比)',
                           'volume_chg_pct': '成交量变更(百分比)', 'amount_chg_pct': '成交额变更(百分比)'},

            # 可转债
            # 可转债信息
            'bond_info': {
                'code': '可转债代码', 'name': '可转债简称', 'stock_code': '股票代码', 'stock_name': '股票名称',
                'listing_date': '上市日期', 'is_delist': '是否退市'
            },
            # 可转债日线数据
            'bond_daily': {'code': '代码', 'name': '名称', 'trade_date': '交易日',
                           'close': '收盘价', 'open': '开盘价', 'high': '最高价', 'low': '最低价',
                           'volume': '成交量(股)', 'amount': '成交额(元)',
                           'turnover': '换手率', 'hfq_factor': '后复权因子', 'chg_pct': '涨跌比(百分比)',
                           'volume_chg_pct': '成交量变更(百分比)', 'amount_chg_pct': '成交额变更(百分比)'},

            # 交易日
            'trade_date': {'trade_date': '交易日'}
        }

    async def load_stock_daily(self, fq: str = None, **kwargs) -> Optional[pd.DataFrame]:
        """
        :param fq: qfq 前复权 hfq 后复权 None不复权
        :param kwargs:  filter=None, projection=None, skip=0, limit=0, sort=None, to_frame=True
        :return: DataFrame([code,trade_date,open,high,low,close,vol,amount])
        """
        self.log.debug('加载股票日线, kwargs={}'.format(kwargs))

        proj_tmp = kwargs['projection'] if 'projection' in kwargs else None
        proj = self.meta['stock_daily'].keys()
        kwargs['projection'] = proj

        df = await self.do_load(self.stock_daily, **kwargs)
        if df is None or df.shape[0] == 0:
            self.log.debug('加载日线数据成功 size=0')
            return None

        # 需按trade_date升序
        if fq == 'qfq' or fq == 'hfq':
            if fq == 'qfq':
                raise Exception('前复权不常用，计算不方便，有需要自己计算，此函数不支持')

            if fq == 'hfq':
                df['open'] = df['open'] * df['hfq_factor']
                df['high'] = df['high'] * df['hfq_factor']
                df['low'] = df['low'] * df['hfq_factor']
                df['close'] = df['close'] * df['hfq_factor']
                df['volume'] = df['volume'] * df['hfq_factor']

        if proj_tmp is not None:
            df = df[proj_tmp]
        self.log.debug('加载日线数据成功 size={}'.format(df.shape[0]))
        return df

    async def next_trade_date(self, test_date: datetime, days=1) -> Optional[datetime]:
        test_date = test_date.year * 10000 + test_date.month * 100 + test_date.day

        df = await self.do_load(self.trade_date,
                                filter={'trade_date': {'$gt': test_date}},
                                limit=days,
                                sort=[('trade_date', 1)]
                                )
        if df is None or df.shape[0] != days:
            return None

        return datetime.strptime(str(df.iloc[days-1]['trade_date']), '%Y%m%d')

    async def prev_trade_date(self, test_date: datetime, days=1) -> Optional[datetime]:
        test_date = test_date.year * 10000 + test_date.month * 100 + test_date.day

        df = await self.do_load(self.trade_date,
                                filter={'trade_date': {'$lt': test_date}},
                                limit=days,
                                sort=[('trade_date', -1)]
                                )
        if df is None or df.shape[0] != days:
            return None

        return datetime.strptime(str(df.iloc[days-1]['trade_date']), '%Y%m%d')

    async def is_trade_date(self, test_date) -> Optional[datetime]:
        test_date = test_date.year * 10000 + test_date.month * 100 + test_date.day

        df = await self.do_load(self.trade_date,
                                filter={'trade_date': test_date},
                                limit=1,
                                sort=[('trade_date', -1)]
                                )
        if df is None or df.shape[0] == 0:
            return False

        return True


if __name__ == '__main__':
    from winq.common import run_until_complete
    from datetime import datetime

    async def my_count(db):
        count = await db.stock_info.count_documents({})
        print('count={}'.format(count))

    async def stock_info(db):
        df = await db.load_stock_info(limit=5, sort=[('listing_date', -1)])
        print(df)

    async def row_load(db):
        n = datetime.strptime('20200818', '%Y%m%d')
        ft = {'code': 'sh688519'}
        df = await db.load_stock_daily(filter=ft, limit=10, sort=[('trade_date', -1)])
        print(df)
        
    async def trade_date(db):
        n = datetime.strptime('20230324', '%Y%m%d')
        
        t = await db.is_trade_date(n)
        print('{} is_trade_date: {}'.format(n, t))
        
        t = await db.next_trade_date(n)
        print('{} next_trade_date: {}'.format(n, t))
        
        t = await db.next_trade_date(n, 3)
        print('{} next_trade_date(3): {}'.format(n, t))
        
        t = await db.pre_trade_date(n)
        print('{} pre_trade_date: {}'.format(n, t))
        
        t = await db.pre_trade_date(n, 3)
        print('{} pre_trade_date(3): {}'.format(n, t))

    db = WinQDB(uri='mongodb://localhost:27017/')
    db.init()
    run_until_complete(
        trade_date(db)
    )
