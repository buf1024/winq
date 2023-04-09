from os.path import dirname
from functools import partial
from winq.common import prepare_strategy, get_strategy

__strategies = dict(builtin=dict(), external=dict())

init_strategy = partial(prepare_strategy, __strategies, (dirname(__file__), 'winq.trade.strategy', ('strategy.py',)))
get_trade_strategy = partial(get_strategy, __strategies)
