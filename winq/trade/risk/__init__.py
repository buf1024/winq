from os.path import dirname
from functools import partial
from winq.common import prepare_strategy, get_strategy

__risks = dict(builtin=dict(), external=dict())

init_risk = partial(prepare_strategy, __risks, (dirname(__file__), 'winq.trade.risk', ('risk.py', )))
get_risk = partial(get_strategy, __risks)
