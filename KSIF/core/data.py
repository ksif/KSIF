"""

"""

from KSIF.core import ffn
import KSIF.core.utils as utils
import pandas as pd
from pandas_datareader import data as pdata

__author__ = 'Seung Hyeon Yu'
__email__ = 'rambor12@business.kaist.ac.kr'


@utils.memoize
def get(tickers, provider=None, source='yahoo', common_dates=True, forward_fill=False,
        clean_tickers=True, column_names=None, ticker_field_sep=';',
        mrefresh=False, merge_to=None, **kwargs):
    """
    Helper function for retrieving data as a DataFrame.

    Args:
        * tickers (list, string, csv string): Tickers to download.
        * provider (function): Provider to use for downloading data.
            By default it will be ffn.DEFAULT_PROVIDER if not provided.
        * common_dates (bool): Keep common dates only? Drop na's.
        * forward_fill (bool): forward fill values if missing. Only works
            if common_dates is False, since common_dates will remove
            all nan's, so no filling forward necessary.
        * clean_tickers (bool): Should the tickers be 'cleaned' using
            ffn.utils.clean_tickers? Basically remove non-standard
            characters (^VIX -> vix) and standardize to lower case.
        * column_names (list): List of column names if clean_tickers
            is not satisfactory.
        * ticker_field_sep (char): separator used to determine the
            ticker and field. This is in case we want to specify
            particular, non-default fields. For example, we might
            want: AAPL;Low,AAPL;High,AAPL;Close. ';' is the separator.
        * mrefresh (bool): Ignore memoization.
        * merge_to (DataFrame): Existing DataFrame to append returns
            to - used when we download from multiple sources
        * kwargs: passed to provider

    """

    if provider is None:
        provider = DEFAULT_PROVIDER
        if 'path' in kwargs:
            provider = csv

    # load csv if the tickers is a path and set 'date' as index
    if '.csv' in tickers:
        df = pd.read_csv(tickers, encoding='euc-kr', **kwargs)
        if ('DATE' in df.columns) or ('date' in df.columns):
            if 'date' in df.columns:
                df = df.rename(columns={'date':'DATE'})
            df = df.set_index(df.columns[df.columns.get_loc('DATE')])
        else:
            df = df.set_index(df.columns[0])
        try:
            df.index = pd.to_datetime(df.index)
        except:
            pass
        mask = list(set(df.columns[df.dtypes == object]))
        df[mask] = df[mask].applymap(utils.to_numeric)
        return df

    tickers = utils.parse_arg(tickers)

    data = {}
    for ticker in tickers:
        t = ticker
        f = None

        # check for field
        bits = ticker.split(ticker_field_sep, 1)
        if len(bits) == 2:
            t = bits[0]
            f = bits[1]

        # call provider - check if supports memoization
        if hasattr(provider, 'mcache'):
            data[ticker] = provider(ticker=t, field=f, source=source,
                                    mrefresh=mrefresh, **kwargs)
        else:
            data[ticker] = provider(ticker=t, field=f, source=source,
                                    **kwargs)

    df = pd.DataFrame(data)
    # ensure same order as provided
    df = df[tickers]

    if merge_to is not None:
        df = ffn.merge(merge_to, df)

    if common_dates:
        df = df.dropna()

    if forward_fill:
        df = df.fillna(method='ffill')

    if column_names:
        cnames = utils.parse_arg(column_names)
        if len(cnames) != len(df.columns):
            raise ValueError(
                'column_names must be of same length as tickers')
        df.columns = cnames
    elif clean_tickers:
        df.columns = map(utils.clean_ticker, df.columns)

    return df


@utils.memoize
def web(ticker, field=None, start=None, end=None,
        mrefresh=False, source='yahoo'):
    """
    Data provider wrapper around pandas.io.data provider. Provides
    memoization.
    """
    new_ticker, source = korean_ticker(ticker, source)
    if source == 'yahoo' and field is None:
        field = 'Adj Close'
    elif source == 'google' and field is None:
        field = 'Close'

    tmp = _download_web(new_ticker, data_source=source,
                        start=start, end=end)

    if tmp is None:
        raise ValueError('failed to retrieve data for %s:%s' % (ticker, field))

    if field:
        return tmp[field]
    else:
        return tmp


def korean_ticker(name, source):
    """
    Return korean index ticker within the source.

    :param name: (str) index name
    :param source: (str) source name
    :return: (str) ticker for the source
    """
    n = name.lower().replace(' ', '').replace('_','')
    if source == 'yahoo':
        if n == 'kospi':
            return '^KS11', source
        if n == 'kosdaq':
            return '^KQ11', source
        if n == 'kospi200':
            return 'KRX:KOSPI200', 'google'
        if n == 'kospi100':
            return 'KRX:KOSPI100', 'google'
        if n == 'kospi50':
            return 'KRX:KOSPI50', 'google'
        if n == 'kospilarge':
            return 'KRX:KOSPI-2', 'google'
        if n == 'kospimiddle':
            return 'KRX:KOSPI-3', 'google'
        if n == 'kospismall':
            return 'KRX:KOSPI-4', 'google'

    if source == 'google':
        if n == 'kospi':
            return 'KRX:KOSPI', source

    return name, source


@utils.memoize
def _download_web(name, **kwargs):
    """
    Thin wrapper to enable memoization
    """
    return pdata.DataReader(name, **kwargs)


@utils.memoize
def csv(ticker, path='data.csv', field='', mrefresh=False, source=None, **kwargs):
    """
    Data provider wrapper around pandas' read_csv. Provides memoization.
    """
    # set defaults if not specified
    if 'index_col' not in kwargs:
        kwargs['index_col'] = 0
    if 'parse_dates' not in kwargs:
        kwargs['parse_dates'] = True

    # read in dataframe from csv file
    df = pd.read_csv(path, **kwargs)

    tf = ticker
    if field is not '' and field is not None:
        tf = '%s:%s' % (tf, field)

    # check that required column exists
    if tf not in df:
        raise ValueError('Ticker(field) not present in csv file!')

    return df[tf]


DEFAULT_PROVIDER = web
