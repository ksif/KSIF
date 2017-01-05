"""
KSIF

Backtest library for KSIF.

"""
from . import core
from .core import base, algos, backtest, ffn, data, utils

from .core.backtest import Backtest, run
from .core.base import Strategy, Algo, AlgoStack
from .core.algos import run_always
from .core.ffn import utils, merge
from .core.data import get

core.ffn.extend_pandas()

__version__ = (0, 1, 0)
__author__ = 'Seung Hyeon Yu'
__email__ = 'rambor12@business.kaist.ac.kr'