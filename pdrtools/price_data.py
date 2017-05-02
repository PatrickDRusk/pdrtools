#! /usr/bin/env python

import cPickle
import datetime
import numbers
import os
import time
from collections import OrderedDict

import boto3
import numpy
import pandas
from pandas.tseries import offsets

import maquila
from instrumentz import contract, security, series
from pandaux import indaux

current_millis = lambda: int(round(time.time() * 1000))

BACKTEST_START = '1990-01-06'
BACKTEST_END = str(datetime.date.today())
BACKTEST_INDEX = pandas.period_range(start=BACKTEST_START, end=BACKTEST_END, freq='B')

SEVEN_YRS = ((7 * 260) * offsets.BDay())
ALMOST_10_YRS = (((9 * 260) + 220) * offsets.BDay())

DEEP_PAST = BACKTEST_INDEX[0]
TODAY = BACKTEST_INDEX[-1]
DEEP_FUTURE = indaux.apply_offset(TODAY, ALMOST_10_YRS)

WRITE_CONTRACT_DATA = False

COMDTYS = (
    "CO_COMDTY",
)

MAQUILAS = (
    "CO_COMDTY:FRONT:V1",
)

BUCKET = None
PREFIX = 'pdr/blobz2'


def write_blob(sec_name, contract_name, blob_name, data):
    if contract_name is not None:
        path = os.path.join(PREFIX, sec_name, contract_name, blob_name)
    else:
        path = os.path.join(PREFIX, sec_name, blob_name)
    print("Skipping blob write")
    #print data
    #pstr = cPickle.dumps(data, -1)
    #BUCKET.put_object(Key=path, Body=pstr)


def read_blob(sec_name, contract_name, blob_name):
    if contract_name is not None:
        path = os.path.join(PREFIX, sec_name, contract_name, blob_name)
    else:
        path = os.path.join(PREFIX, sec_name, blob_name)
    pstr = BUCKET.Object(path).get().get('Body').read()
    data = cPickle.loads(pstr)
    return data


def load_contract(sec_name, contract_name, inception, expiry, open_, close, volume, high, low, open_interest):
    if (expiry is None) or (isinstance(expiry, numbers.Number) and numpy.isnan(expiry)):
        raise ValueError("%s has no inception date" % contract_name)
    if (inception is None) or (isinstance(inception, numbers.Number) and numpy.isnan(inception)):
        raise ValueError("%s has no inception date" % contract_name)

    contract_df = pandas.DataFrame.from_dict(
        OrderedDict((
            ('contract', contract_name),
            ('open', open_),
            ('close', close),
            ('volume', volume),
            ('high', high),
            ('low', low),
            ('open_interest', open_interest))
        )
    )
    contract_df.index.name = 'log_s'

    # Check for zeroes
    zeroes_df = contract_df[contract_df.close == 0.0]
    if len(zeroes_df):
        vols_df = zeroes_df[zeroes_df.Volume > 0.0]
        print "%s has %d zero prices, %d with positive volumes" % (contract_name, len(zeroes_df), len(vols_df))
        contract_df = contract_df[~(contract_df.close == 0.0)]

    if WRITE_CONTRACT_DATA:
        print "Writing contract data for %s" % contract_name
        load_df(sec_name, contract_name, contract_df, ('open', 'close', 'volume', 'high', 'low', 'open_interest'))

    return contract_df


def load_df(sec_name, contract_name, df, columns):
    obs_keys = df.index.to_timestamp() + (6 * offsets.Hour())
    op_keys = obs_keys + offsets.Hour()

    df.insert(0, 'obs', obs_keys)
    df.insert(1, 'op', op_keys)

    write_blob(sec_name, contract_name, 'all', df)

    for col in columns:
        new_df = df.copy().loc[:, (col,)]
        new_df.columns = ['value']
        new_df = new_df[~numpy.isnan(new_df.value)]
        write_blob(sec_name, contract_name, col, new_df)


def load_security_data(sec_name, df, columns):
    df = df.reset_index()
    df = df.set_index(['contract', 'log_s'])
    write_blob(sec_name, None, 'all', df)
    for col in columns:
        new_df = df.copy().loc[:, (col,)]
        new_df.columns = ['value']
        new_df = new_df[~numpy.isnan(new_df.value)]
        write_blob(sec_name, None, col, new_df)

    df.to_csv('spaz.csv')


def load_symbol(sec_name):
    instrument = security.factory(sec_name)

    md_dict = dict()
    for key in instrument.metadata.keys():
        md_dict[key] = instrument.metadata[key]
    #write_blob(sec_name, None, 'metadata', md_dict)

    # Create a frame from the expiry map, which will have contract names as the index
    df = pandas.Series(md_dict['expiry_map'])
    df = df.map(lambda x: pandas.Period(x, freq="B"))
    df = df.to_frame('expiration')

    # Add a column for the inception dates
    try:
        contract_inception = pandas.Series(md_dict['inception_map'])
        contract_inception = contract_inception.map(lambda x: pandas.Period(x, freq="B"))
        df.loc[:, 'inception'] = contract_inception
        df.insert(0, 'security', sec_name)
        df.index.name = 'contract'
    except KeyError as exc:
        print "%s has no inception map" % sec_name
        raise exc

    #df.to_csv('metadata.csv')

    # Iterate over the contracts looking for bad prices
    all_df = None
    for contract_name in df.index:
        inception = df.loc[contract_name, 'inception']
        expiry = df.loc[contract_name, 'expiration']

        cindex = pandas.period_range(start=indaux.apply_offset(inception, -offsets.BDay()),
                                     end=indaux.apply_offset(expiry, offsets.BDay()),
                                     freq='B')

        # If there is any close or volume data, construct a dataframe with those rows
        try:
            open_ = series.daily_open(contract.factory(contract_name), cindex).dropna()
            close = series.daily_close(contract.factory(contract_name), cindex).dropna()
            high = series.daily_high(contract.factory(contract_name), cindex).dropna()
            low = series.daily_low(contract.factory(contract_name), cindex).dropna()
            volume = series.daily_volume(contract.factory(contract_name), cindex).dropna()
            open_interest = series.daily_open_interest(contract.factory(contract_name), cindex).dropna()
            if len(close) or len(volume):
                foo = load_contract(sec_name, contract_name, inception, expiry,
                                    open_, close, volume, high, low, open_interest)
                if all_df is None:
                    all_df = foo
                else:
                    all_df = pandas.concat([all_df, foo])
        except Exception as exc:
            print exc

    load_security_data(sec_name, all_df, ('open', 'close', 'volume', 'high', 'low', 'open_interest'))


def load_maquila(maquila_key, index):
    sec_expr = maquila.get_repository().instrument(maquila_key)

    # If there is any close or volume data, construct a dataframe with those rows
    try:
        returns = series.daily_returns(sec_expr, index).dropna()
        if len(returns):
            df = pandas.DataFrame.from_dict(
                OrderedDict((
                    ('return', returns),
                ))
            )
            df.index.name = 'log_s'
            load_df(maquila_key, None, df, ('return',))
    except Exception as exc:
        print exc


def log_millis(millis, pattern):
    delta = current_millis() - millis
    print(pattern + str(delta))
    return current_millis()


def read_symbol(sec_name):
    millis = current_millis()
    md_dict = read_blob(sec_name, None, 'metadata')
    millis = log_millis(millis, "Time to read %s metadata: " % sec_name)

    # Iterate over the contracts looking for bad prices
    contract_names = sorted(md_dict['expiry_map'].keys())
    for contract_name in contract_names:
        contract_df = read_blob(sec_name, contract_name, 'close')

    log_millis(millis, "Time to read all %s contract prices: " % sec_name)



def read_symbol_big(sec_name):
    millis = current_millis()
    md_dict = read_blob(sec_name, None, 'metadata')
    millis = log_millis(millis, "Time to read %s metadata: " % sec_name)

    # Iterate over the contracts looking for bad prices
    contract_names = sorted(md_dict['expiry_map'].keys())
    for contract_name in contract_names:
        contract_df = read_blob(sec_name, contract_name, 'all')

    log_millis(millis, "Time to read all %s contract prices (big): " % sec_name)


def read_maquila(maquila_key, col='return'):
    millis = current_millis()
    data = read_blob(maquila_key, None, col)
    log_millis(millis, "Time to read %s returns: " % maquila_key)
    return data


def __main():
    global BUCKET
    s3 = boto3.resource('s3')
    BUCKET = s3.Bucket('cm-engineers')

    #for sec_name in COMDTYS:
    #    load_symbol(sec_name)

    #for maquila_key in MAQUILAS:
    #    load_maquila(maquila_key, BACKTEST_INDEX)

    for sec_name in COMDTYS:
        read_blob(sec_name, None, 'metadata')   # to prime the timings
        print read_maquila(sec_name, col='all')
        # read_symbol(sec_name)
        # read_symbol_big(sec_name)

    for maquila_key in MAQUILAS:
        read_maquila(maquila_key)


if __name__ == '__main__':
    __main()
