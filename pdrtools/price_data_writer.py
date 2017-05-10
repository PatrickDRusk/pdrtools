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

from instrumentz import contract, security, series
from pandaux import indaux

from price_data import storage as price

current_millis = lambda: int(round(time.time() * 1000))

BUCKET = None
PREFIX = 'pdr/blobz4'

CONTRACT_PRICES_IN_SECURITY = 150

DO_WRITES = True
WRITE_CONTRACT_DATA = False
FAKE_SECURITY_CONTRACT = "0000"

BACKTEST_START = '1990-01-06'
BACKTEST_END = str(datetime.date.today())
BACKTEST_INDEX = pandas.period_range(start=BACKTEST_START, end=BACKTEST_END, freq='B')

SEVEN_YRS = ((7 * 260) * offsets.BDay())
ALMOST_10_YRS = (((9 * 260) + 220) * offsets.BDay())

DEEP_PAST = BACKTEST_INDEX[0]
TODAY = BACKTEST_INDEX[-1]
DEEP_FUTURE = indaux.apply_offset(TODAY, ALMOST_10_YRS)

COMDTYS = (
    "AA_COMDTY",
    "CL_COMDTY",
    "CO_COMDTY",
    "CU_COMDTY",
    "HG_COMDTY",
    "LA_COMDTY",
    "LC_COMDTY",
    "LH_COMDTY",
    "LL_COMDTY",
    "LN_COMDTY",
    "LP_COMDTY",
    "LX_COMDTY",

    "BO_COMDTY",
    "BZA_COMDTY",
    "CC_COMDTY",
    "CT_COMDTY",
    "EN_COMDTY",
    "ES_INDEX",
    "GC_COMDTY",
    "HO_COMDTY",
    "KC_COMDTY",
    "NG_COMDTY",
    "PL_COMDTY",
    "QS_COMDTY",
    "SB_COMDTY",
    "SM_COMDTY",
    "S_COMDTY",
    "TY_COMDTY",
    "W_COMDTY",
    "XB_COMDTY",
)

BAD_CONTRACTS = {
    "AAX99_COMDTY",
    "AAZ99_COMDTY",
    "AAF00_COMDTY",
    "AAG00_COMDTY",
    "AAH00_COMDTY",
    "AAK00_COMDTY",
    "AAJ00_COMDTY",
    "AAM00_COMDTY",
    "AAN00_COMDTY",
    "AAU00_COMDTY",
    "AAQ00_COMDTY",
}


def write_blob(sec_name, contract_name, category, blob_name, data):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, blob_name]))
    if DO_WRITES:
        print(path)
        pstr = cPickle.dumps(data, -1)
        BUCKET.put_object(Key=path, Body=pstr)
    else:
        print("Skipping blob write")
        # print data


def write_df(sec_name, contract_name, category, blob_name, df):
    df = df.copy().reset_index()
    # Convert log_s to a column of POSIX timestamp integers
    df.log_s = [int(time.mktime(p.to_timestamp().to_datetime().timetuple())) for p in df.log_s]
    column_names = tuple(df.columns)
    column_data = list()
    for col_name in column_names:
        column_data.append(df.loc[:, col_name].values)
    df_dict = dict(names=column_names, data=column_data)
    write_blob(sec_name, contract_name, category, blob_name, df_dict)


def write_df_and_columns(sec_name, contract_name, category, df, columns):
    # obs_keys = df.index.to_timestamp() + (6 * offsets.Hour())
    # op_keys = obs_keys + offsets.Hour()

    # df.insert(0, 'obs', obs_keys)
    # df.insert(1, 'op', op_keys)

    write_df(sec_name, contract_name, category, 'all', df)

    for col in columns:
        new_df = df.copy().loc[:, (col,)]
        new_df.columns = ['value']
        new_df = new_df[~numpy.isnan(new_df.value)]
        write_df(sec_name, contract_name, category, col, new_df)


def write_contract(sec_name, contract_name, category, inception, expiry, close, volume):
    if (expiry is None) or (isinstance(expiry, numbers.Number) and numpy.isnan(expiry)):
        raise ValueError("%s has no expiration date" % contract_name)
    if (inception is None) or (isinstance(inception, numbers.Number) and numpy.isnan(inception)):
        #raise ValueError("%s has no inception date" % contract_name)
        print("%s has no inception date" % contract_name)

    contract_df = pandas.DataFrame.from_dict(
        OrderedDict((
            ('contract', contract_name),
            ('close', close),
            ('volume', volume),
            )
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
        write_df_and_columns(sec_name, contract_name, category, contract_df, ('close', 'volume'))
    else:
        print '.', # contract_name,

    return contract_df


def write_security(sec_name, df, category, columns):
    df = df.reset_index()
    df = df.set_index(['contract', 'log_s'])
    write_df(sec_name, FAKE_SECURITY_CONTRACT, category, 'all', df)
    for col in columns:
        new_df = df.copy().loc[:, (col,)]
        new_df.columns = ['value']
        new_df = new_df[~numpy.isnan(new_df.value)]
        write_df(sec_name, FAKE_SECURITY_CONTRACT, category, col, new_df)

    # df.to_csv('spaz.csv')


def process_symbol(sec_name):
    instrument = security.factory(sec_name)

    md_dict = dict()
    for key in instrument.metadata.keys():
        md_dict[key] = instrument.metadata[key]
    write_blob(sec_name, None, None, 'metadata', md_dict)

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

    # df.to_csv('metadata.csv')

    # Iterate over the contracts looking for bad prices
    all_df = None
    for contract_name in df.index:
        if contract_name in BAD_CONTRACTS:
            continue

        inception = df.loc[contract_name, 'inception']
        expiry = df.loc[contract_name, 'expiration']

        end = indaux.apply_offset(expiry, offsets.BDay())
        if end <= DEEP_PAST:
            continue

        if inception:
            try:
                start = indaux.apply_offset(inception, -offsets.BDay())
            except:
                start = DEEP_PAST
        else:
            start = DEEP_PAST

        cindex = pandas.period_range(start=start, end=end, freq='B')

        # If there is any close or volume data, construct a dataframe with those rows
        try:
            close = series.daily_close(contract.factory(contract_name), cindex).dropna()
            volume = series.daily_volume(contract.factory(contract_name), cindex).dropna()
            if len(close) or len(volume):
                foo = write_contract(sec_name, contract_name, "DAILY", inception, expiry, close, volume)
                foo = foo.iloc[-CONTRACT_PRICES_IN_SECURITY:, :]
                if all_df is None:
                    all_df = foo
                else:
                    all_df = pandas.concat([all_df, foo])
        except Exception as exc:
            print exc

    print

    if all_df is not None:
        # noinspection PyTypeChecker
        write_security(sec_name, all_df, "DAILY", ('close', 'volume'))


def log_millis(millis, pattern):
    delta = current_millis() - millis
    print(pattern + str(delta))
    return current_millis()


def __main():
    millis = current_millis()

    global BUCKET
    s3 = boto3.resource('s3')
    BUCKET = s3.Bucket('cm-engineers')

    for sec_name in COMDTYS:
        process_symbol(sec_name)

    log_millis(millis, "Time to run: ")


if __name__ == '__main__':
    __main()
