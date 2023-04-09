from os.path import dirname
from functools import partial
from winq.common import prepare_strategy, get_strategy

__robots = dict(builtin=dict(), external=dict())

init_robot = partial(prepare_strategy, __robots, (dirname(__file__), 'winq.trade.robot', ('robot.py', )))
get_robot = partial(get_strategy, __robots)

