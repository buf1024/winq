from .base_obj import BaseObj
from winq.trade.tradedb import TradeDB
from winq.data import *
import winq.trade.consts as consts


class BaseActionObj(BaseObj):
    def __init__(self, typ: str, db_data: MongoDB, db_trade: TradeDB, trader):
        super().__init__(typ=typ, db_data=db_data, db_trade=db_trade, trader=trader)

    def can_buy(self, code: str, price: float, volume: int) -> bool:
        if volume <= 0:
            return False
        cost = self.trader.account.get_cost(typ=consts.act_buy, code=code, price=price, volume=volume)
        return self.trader.account.cash_available >= cost

    def can_sell_volume(self, code: str) -> int:
        _, vol = self.trader.account.get_position_volume(code=code)
        return vol

    def can_sell(self, code: str) -> int:
        _, vol = self.trader.account.get_position_volume(code=code)
        return vol > 0

    def can_cancel(self, code: str):
        entrusts = self.trader.account.get_active_entrust(code=code)
        return entrusts

    async def buy(self, sig):
        await self.emit('signal', consts.evt_sig_buy, sig)

    async def sell(self, sig):
        await self.emit('signal', consts.evt_sig_sell, sig)

    async def cancel(self, sig):
        await self.emit('signal', consts.evt_sig_cancel, sig)

    async def quot_subs(self, *codes):
        """
        订阅新的代码行情
        """
        if len(codes) > 0:
            await self.emit('quotation', consts.evt_quot_codes, codes)
