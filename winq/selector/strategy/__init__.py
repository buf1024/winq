from winq.common import load_strategy
from os.path import dirname
from .strategy import Strategy
from .backtest import Backtest
from .data_plot import DataPlot
from .data_with import DataWith, DataWithConcept, DataWithStat

__file_path = dirname(__file__)

strategies = load_strategy(
    __file_path, 'winq.selector.strategy', ('strategy.py', 'comm.py', 'data_plot.py', 'data_with.py', 'backtest.py'))
