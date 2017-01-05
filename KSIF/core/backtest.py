from __future__ import division
from copy import deepcopy
import KSIF as kf
import KSIF.core.ffn as ffn
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import pyprind
import os
from datetime import datetime

__author__ = 'Seung Hyeon Yu'
__email__ = 'rambor12@business.kaist.ac.kr'


DEFAULT_PATH = os.getcwd()


def run(*backtests):
    """
    Runs a series of backtests and returns a Result
    object containing the results of the backtests.

    Args:
        * backtest (*list): List of backtests.


    Returns:
        Result

    """
    # run each backtest
    for bkt in backtests:
        bkt.run()

    return Result(*backtests)


def benchmark_random(backtest, random_strategy, nsim=100):
    """
    Given a backtest and a random strategy, compare backtest to
    a number of random portfolios.

    The idea here is to benchmark your strategy vs a bunch of
    random strategies that have a similar structure but execute
    some part of the logic randomly - basically you are trying to
    determine if your strategy has any merit - does it beat
    randomly picking weight? Or randomly picking the selected
    securities?

    Args:
        * backtest (Backtest): A backtest you want to benchmark
        * random_strategy (Strategy): A strategy you want to benchmark
            against. The strategy should have a random component to
            emulate skilless behavior.
        * nsim (int): number of random strategies to create.

    Returns:
        RandomBenchmarkResult

    """
    # save name for future use
    if backtest.name is None:
        backtest.name = 'original'

    # run if necessary
    if not backtest.has_run:
        backtest.run()

    bts = []
    bts.append(backtest)
    data = backtest.data

    # create and run random backtests
    for i in range(nsim):
        random_strategy.name = 'random_%s' % i
        rbt = kf.Backtest(random_strategy, data)
        rbt.run()

        bts.append(rbt)

    # now create new RandomBenchmarkResult
    res = RandomBenchmarkResult(*bts)

    return res


class Backtest(object):
    """
    A Backtest combines a Strategy with data to
    produce a Result.

    A backtest is basically testing a strategy over a data set.

    Note:
        The Strategy will be deepcopied so it is re-usable in other
        backtests. To access the backtested strategy, simply access
        the strategy attribute.

    Args:
        * strategy (Strategy, Node, StrategyBase): The Strategy to be tested.
        * data (DataFrame): DataFrame containing data used in backtest. This
            will be the Strategy's "universe".
        * name (str): Backtest name - defaults to strategy name
        * initial_capital (float): Initial amount of capital passed to
            Strategy.
        * commissions (fn(quantity)): The commission function to be used.
        * progress_bar (Bool): Display progress bar while running backtest

    Attributes:
        * strategy (Strategy): The Backtest's Strategy. This will be a deepcopy
            of the Strategy that was passed in.
        * data (DataFrame): Data passed in
        * dates (DateTimeIndex): Data's index
        * initial_capital (float): Initial capital
        * name (str): Backtest name
        * stats (ffn.PerformanceStats): Performance statistics
        * has_run (bool): Run flag
        * weights (DataFrame): Weights of each component over time
        * security_weights (DataFrame): Weights of each security as a
            percentage of the whole portfolio over time

    """

    def __init__(self, strategy, data,
                 name=None,
                 initial_capital=1000000.0,
                 commissions=True,
                 integer_positions=True,
                 progress_bar=True):

        if data.columns.duplicated().any():  # data column에 이름 같은게 있는지 체크
            cols = data.columns[data.columns.duplicated().tolist()].tolist()  # 중복되는 column 이름 고르기
            raise Exception(
                'data provided has some duplicate column names: \n%s \n'
                'Please remove duplicates!' % cols)

        # we want to reuse strategy logic - copy it!
        # basically strategy is a template
        self.strategy = deepcopy(strategy)
        self.strategy.use_integer_positions(integer_positions)

        self.data = data
        self.dates = data.index
        self.initial_capital = initial_capital
        self.name = name if name is not None else strategy.name
        self.progress_bar = progress_bar

        if commissions is True or commissions.lower() == 'high':
            self.strategy.set_commissions(commission_high)
        elif commissions is False or commissions is None:
            pass
        elif commissions.lower() == 'low':
            self.strategy.set_commissions(commission_low)
        else:
            self.strategy.set_commissions(commissions)

        self.stats = {}
        self._original_prices = None
        self._weights = None
        self._sweights = None
        self.has_run = False

    def run(self):
        """
        Runs the Backtest.
        """
        # set run flag
        self.has_run = True

        # setup strategy
        self.strategy.setup(self.data)

        # adjust strategy with initial capital
        self.strategy.adjust(self.initial_capital)

        # loop through dates
        # init progress bar
        if self.progress_bar:
            bar = pyprind.ProgBar(len(self.dates), title=self.name, stream=1)

        for dt in self.dates:
            # update progress bar
            if self.progress_bar:
                bar.update()

            # update strategy
            self.strategy.update(dt)

            if not self.strategy.bankrupt:
                self.strategy.run()
                # need update after to save weights, values and such
                self.strategy.update(dt)
            else:
                if self.progress_bar:
                    bar.stop()

        self.stats = self.strategy.prices.calc_perf_stats()
        self._original_prices = self.strategy.prices

    @property
    def weights(self):
        """
        DataFrame of each component's weight over time
        """
        if self._weights is not None:
            return self._weights
        else:
            vals = pd.DataFrame({x.full_name: x.values for x in
                                 self.strategy.members})
            vals = vals.div(self.strategy.values, axis=0)
            self._weights = vals
            return vals

    @property
    def positions(self):
        """
        DataFrame of each component's position over time
        """
        return self.strategy.positions

    @property
    def security_weights(self):
        """
        DataFrame containing weights of each security as a
        percentage of the whole portfolio over time
        """
        if self._sweights is not None:
            return self._sweights
        else:
            # get values for all securities in tree and divide by root values
            # for security weights
            vals = {}
            for m in self.strategy.members:
                if isinstance(m, kf.core.base.SecurityBase):
                    if m.name in vals:
                        vals[m.name] += m.values
                    else:
                        vals[m.name] = m.values
            vals = pd.DataFrame(vals)

            # divide by root strategy values
            vals = vals.div(self.strategy.values, axis=0)

            # save for future use
            self._sweights = vals

            return vals

    @property
    def herfindahl_index(self):
        """
        Calculate Herfindahl-Hirschman Index (HHI) for the portfolio.
        For each given day, HHI is defined as a sum of squared weights of
        securities in a portfolio; and varies from 1/N to 1.
        Value of 1/N would correspond to an equally weighted portfolio and
        value of 1 corresponds to an extreme case when all amount is invested
        in a single asset.

        1 / HHI is often considered as "an effective number of assets" in
        a given portfolio
        """
        w = self.security_weights
        return (w ** 2).sum(axis=1)

    @property
    def turnover(self):
        """
        Calculate the turnover for the backtest.

        This function will calculate the turnover for the strategy. Turnover is
        defined as the lesser of positive or negative outlays divided by NAV
        """
        s = self.strategy
        outlays = s.outlays

        # seperate positive and negative outlays, sum them up, and keep min
        outlaysp = outlays[outlays >= 0].fillna(value=0).sum(axis=1)
        outlaysn = np.abs(outlays[outlays < 0].fillna(value=0).sum(axis=1))

        # merge and keep minimum
        min_outlay = pd.DataFrame(
            {'pos': outlaysp, 'neg': outlaysn}).min(axis=1)

        # turnover is defined as min outlay / nav
        mrg = pd.DataFrame({'outlay': min_outlay, 'nav': s.values})

        return mrg['outlay'] / mrg['nav']

    def _portfolios(self, date=None):
        """
        Return Portfolios.

        This function will calculate the which stocks are in our portfolios.
        :param date: (int, pd.TimeStamp) Cross sectional Date
        :return: (pd.Series) Positions of stocks.
        """
        if date is None:
            return self.positions.stack()

        elif isinstance(date, int):
            return self.positions.ix[date][self.weights.ix[date] != 0]

        elif isinstance(date, pd.TimeStamp):
            idate = self.positions.index.get_loc(date)
            return self._portfolio(idate)

    @property
    def portfolios(self):
        return self._portfolios()

    def portfolio(self, date):
        return self._portfolios(date)


class Result(ffn.GroupStats):
    """
    Based on ffn's GroupStats with a few extra helper methods.

    Args:
        * backtests (list): List of backtests

    Attributes:
        * backtest_list (list): List of bactests in the same order as provided
        * backtests (dict): Dict of backtests by name

    """

    def __init__(self, *backtests):
        tmp = [pd.DataFrame({x.name: x.strategy.prices}) for x in backtests]
        super(Result, self).__init__(*tmp)
        self.backtest_list = backtests
        self.backtests = {x.name: x for x in backtests}

    def display_monthly_returns(self, backtest=0):
        """
        Display monthly returns for a specific backtest.

        Args:
            * backtest (str, int): Backtest. Can be either a index (int) or the
                name (str)

        """
        key = self._get_backtest(backtest)
        self[key].display_monthly_returns()

    def plot_weights(self, backtest=0, filter=None,
                     figsize=(15, 5), **kwds):
        """
        Plots the weights of a given backtest over time.

        Args:
            * backtest (str, int): Backtest. Can be either a index (int) or the
                name (str)
            * filter (list, str): filter columns for specific columns. Filter
                is simply passed as is to DataFrame[filter], so use something
                that makes sense with a DataFrame.
            * figsize ((width, height)): figure size
            * kwds (dict): Keywords passed to plot

        """
        key = self._get_backtest(backtest)

        if filter is not None:
            data = self.backtests[key].weights[filter]
        else:
            data = self.backtests[key].weights

        data.plot(figsize=figsize, **kwds)

    def plot_security_weights(self, backtest=0, filter=None,
                              figsize=(15, 5), **kwds):
        """
        Plots the security weights of a given backtest over time.

        Args:
            * backtest (str, int): Backtest. Can be either a index (int) or the
                name (str)
            * filter (list, str): filter columns for specific columns. Filter
                is simply passed as is to DataFrame[filter], so use something
                that makes sense with a DataFrame.
            * figsize ((width, height)): figure size
            * kwds (dict): Keywords passed to plot

        """
        key = self._get_backtest(backtest)

        if filter is not None:
            data = self.backtests[key].security_weights[filter]
        else:
            data = self.backtests[key].security_weights

        data.plot(figsize=figsize, **kwds)

    def plot_histogram(self, backtest=0, **kwds):
        """
        Plots the return histogram of a given backtest over time.

        Args:
            * backtest (str, int): Backtest. Can be either a index (int) or the
                name (str)
            * kwds (dict): Keywords passed to plot_histogram

        """
        key = self._get_backtest(backtest)
        self[key].plot_histogram(**kwds)

    def _get_backtest(self, backtest):
        # based on input order
        if type(backtest) == int:
            return self.backtest_list[backtest].name

        # default case assume ok
        return backtest

    def save(self, path=DEFAULT_PATH):
        """
        Save the Result using pickle.
        :param path: (str) path to save
        :return: True if succeed
        """
        import pickle
        name = log_name(path, ';'.join(list(self.keys())))
        f = open(path+'\\'+name, 'wb')
        pickle.dump(self, f)
        f.close()
        return True


class RandomBenchmarkResult(Result):
    """
    RandomBenchmarkResult expands on Result to add methods specific
    to random strategy benchmarking.

    Args:
        * backtests (list): List of backtests

    Attributes:
        * base_name (str): Name of backtest being benchmarked
        * r_stats (Result): Stats for random strategies
        * b_stats (Result): Stats for benchmarked strategy

    """

    def __init__(self, *backtests):
        super(RandomBenchmarkResult, self).__init__(*backtests)
        self.base_name = backtests[0].name
        # seperate stats to make
        self.r_stats = self.stats.drop(self.base_name, axis=1)
        self.b_stats = self.stats[self.base_name]

    def plot_histogram(self, statistic='monthly_sharpe',
                       figsize=(15, 5), title=None,
                       bins=20, **kwargs):
        """
        Plots the distribution of a given statistic. The histogram
        represents the distribution of the random strategies' statistic
        and the vertical line is the value of the benchmarked strategy's
        statistic.

        This helps you determine if your strategy is statistically 'better'
        than the random versions.

        Args:
            * statistic (str): Statistic - any numeric statistic in
                Result is valid.
            * figsize ((x, y)): Figure size
            * title (str): Chart title
            * bins (int): Number of bins
            * kwargs (dict): Passed to pandas hist function.

        """
        if statistic not in self.r_stats.index:
            raise ValueError("Invalid statistic. Valid statistics"
                             "are the statistics in self.stats")

        if title is None:
            title = '%s histogram' % statistic

        plt.figure(figsize=figsize)

        ser = self.r_stats.ix[statistic]

        ax = ser.hist(bins=bins, figsize=figsize, normed=True, **kwargs)
        ax.set_title(title)
        plt.axvline(self.b_stats[statistic], linewidth=4)
        ser.plot(kind='kde')


def commission_low(q, p):
    """
    한국투자증권 수수료(낮을 때)

    :param q: (int) 매수 주식 수
    :param p: (float) 매수 주식 가격
    :return: (float) 수수료 + 세금
    """
    x = abs(q * p)
    if q < 0:
        tax = int(x * 0.003)
    else:
        tax = 0
    return x * 0.00024164 // 10 * 10 + tax


def commission_high(q, p):
    """
    한국투자증권 수수료(높을 때)

    :param q: (int) 매수 주식 수
    :param p: (float) 매수 주식 가격
    :return: (float) 수수료 + 세금
    """
    x = abs(q * p)
    if q < 0:
        tax = int(x * 0.003)
    else:
        tax = 0
    if x < 500000:
        cost = x * 0.00502704
    elif x < 3000000:
        cost = x * 0.00127296 + 2000
    elif x < 30000000:
        cost = x * 0.00127296 + 1500
    elif x < 100000000:
        cost = x * 0.00117296
    elif x < 300000000:
        cost = x * 0.00097296
    else:
        cost = x * 0.00077296
    return cost // 10 * 10 + tax


def log_name(path, name):
    """
    get file name that is not overlapping.

    :param path: (str) path
    :param name: (str) name of strategy
    :return: (str) file name
    """
    file_list = os.listdir(path)
    now = datetime.now().strftime("%Y-%m-%d")
    front_name = now + '_' + name + '_'
    num = 0

    for file_name in file_list:
        if front_name in file_name:
            buf = int(file_name.replace(front_name, '').replace('.log', '')) + 1
            if buf > num:
                num = buf
    return front_name + str(num).zfill(5) + '.log'


"""
class Summary(Result):


    The Summary is a summary of Result class.
    The Result has too big to store, so we have to pick up only useful data.
    You can reuse the strategy in Summary to backtest with new data.


    def __init__(self, result, summary='', detail='', **kwargs):

        super(Summary, self).__init__(result.backtest_list)

        # Strategy data
        self.names = list(result.keys())
        if summary == '':
            self.summary = [summary for _ in self.names]
        if detail == '':
            self.detail = [detail for _ in self.names]
        # Use algo name as class name
        # algo 속에는 signal 데이터도 포함되어 수백 KB ~ 수 MB 까지 용량 있음
        self.algos = [{algo.__class__.__name__: algo
                      for algo in result.backtests[name].strategy.stack.algos}
                      for name in self.names]
        # 전략 초기화하여 용량 축소
        self.strategies = deepcopy(backtest.strategy).setup(pd.DataFrame({}))

        # Backtest result
        self.stats = backtest.stats
        self.prices = backtest.strategy.prices
        self.portfolios = backtest.portfolios

        for k, v in kwargs.values():
            setattr(self, k, v)
"""