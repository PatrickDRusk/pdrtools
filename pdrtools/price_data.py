#! /usr/bin/env python

import cPickle
import concurrent.futures
import datetime
import numbers
import os
import time
from collections import OrderedDict
from Queue import Queue
from time import sleep

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

    #"BZAM16_COMDTY",
}

MAQUILAS = (
    #"CO_COMDTY:FRONT:V1",
)

BUCKET = None
PREFIX = 'pdr/blobz'

DO_WRITES = False
WRITE_CONTRACT_DATA = False


def write_blob(sec_name, contract_name, category, blob_name, data):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, blob_name]))
    if DO_WRITES:
        print(path)
        pstr = cPickle.dumps(data, -1)
        BUCKET.put_object(Key=path, Body=pstr)
    else:
        print("Skipping blob write")
        # print data


def read_blob(sec_name, contract_name, category, blob_name):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, blob_name]))
    pstr = BUCKET.Object(path).get().get('Body').read()
    data = cPickle.loads(pstr)
    return data


def load_contract(sec_name, contract_name, category, inception, expiry, open_, close, volume, high, low, open_interest):
    if (expiry is None) or (isinstance(expiry, numbers.Number) and numpy.isnan(expiry)):
        raise ValueError("%s has no inception date" % contract_name)
    if (inception is None) or (isinstance(inception, numbers.Number) and numpy.isnan(inception)):
        raise ValueError("%s has no inception date" % contract_name)

    contract_df = pandas.DataFrame.from_dict(
        OrderedDict((
            ('contract', contract_name),
            # ('open', open_),
            ('close', close),
            ('volume', volume),
            # ('high', high),
            # ('low', low),
            # ('open_interest', open_interest)
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
        load_df(sec_name, contract_name, category, contract_df, ('close', 'volume'))
    else:
        print '.', # contract_name,

    return contract_df


def load_df(sec_name, contract_name, category, df, columns):
    obs_keys = df.index.to_timestamp() + (6 * offsets.Hour())
    op_keys = obs_keys + offsets.Hour()

    df.insert(0, 'obs', obs_keys)
    df.insert(1, 'op', op_keys)

    write_blob(sec_name, contract_name, category, 'all', df)

    for col in columns:
        new_df = df.copy().loc[:, (col,)]
        new_df.columns = ['value']
        new_df = new_df[~numpy.isnan(new_df.value)]
        write_blob(sec_name, contract_name, category, col, new_df)


def load_security_data(sec_name, df, category, columns):
    df = df.reset_index()
    df = df.set_index(['contract', 'log_s'])
    write_blob(sec_name, None, category, 'all', df)
    for col in columns:
        new_df = df.copy().loc[:, (col,)]
        new_df.columns = ['value']
        new_df = new_df[~numpy.isnan(new_df.value)]
        write_blob(sec_name, None, category, col, new_df)

    # df.to_csv('spaz.csv')


def load_symbol(sec_name):
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
            # open_ = series.daily_open(contract.factory(contract_name), cindex).dropna()
            close = series.daily_close(contract.factory(contract_name), cindex).dropna()
            # high = series.daily_high(contract.factory(contract_name), cindex).dropna()
            # low = series.daily_low(contract.factory(contract_name), cindex).dropna()
            volume = series.daily_volume(contract.factory(contract_name), cindex).dropna()
            # open_interest = series.daily_open_interest(contract.factory(contract_name), cindex).dropna()
            if len(close) or len(volume):
                foo = load_contract(sec_name, contract_name, "DAILY", inception, expiry,
                                    None, close, volume, None, None, None)
                if all_df is None:
                    all_df = foo
                else:
                    all_df = pandas.concat([all_df, foo])
        except Exception as exc:
            print exc

    print

    if all_df is not None:
        load_security_data(sec_name, all_df, "DAILY", ('close', 'volume'))


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
            load_df(maquila_key, None, "DAILY", df, ('return',))
    except Exception as exc:
        print exc


def log_millis(millis, pattern):
    delta = current_millis() - millis
    print(pattern + str(delta))
    return current_millis()


def read_symbol(sec_name):
    millis = current_millis()
    md_dict = read_blob(sec_name, None, None, 'metadata')
    millis = log_millis(millis, "Time to read %s metadata: " % sec_name)

    contract_names = sorted(md_dict['expiry_map'].keys())
    for contract_name in contract_names:
        # noinspection PyUnusedLocal
        contract_df = read_blob(sec_name, contract_name, "DAILY", 'close')

    log_millis(millis, "Time to read all %s contract prices: " % sec_name)


def read_symbol_big(sec_name):
    millis = current_millis()
    md_dict = read_blob(sec_name, None, None, 'metadata')
    millis = log_millis(millis, "Time to read %s metadata: " % sec_name)

    contract_names = sorted(md_dict['expiry_map'].keys())
    for contract_name in contract_names:
        # noinspection PyUnusedLocal
        contract_df = read_blob(sec_name, contract_name, "DAILY", 'all')

    log_millis(millis, "Time to read all %s contract prices (big): " % sec_name)


def read_maquila(sec_name, contract_name=None, col='close'):
    millis = current_millis()
    data = read_blob(sec_name, contract_name, "DAILY", col)
    log_millis(millis, "Time to read %s returns: " % sec_name)
    return data


def read_threaded(queue, sec_name, contract_name=None, category='DAILY', col='close'):
    print("Starting to read %s" % sec_name)
    millis = current_millis()
    session = boto3.session.Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket('cm-engineers')
    millis = log_millis(millis, "Time to create session and bucket: ")
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, col]))
    pstr = bucket.Object(path).get().get('Body').read()
    data = cPickle.loads(pstr)
    queue.put(data)
    print("Done reading %s" % sec_name)

def read_threaded2(queue, sec_name, contract_name=None, category='DAILY', col='close'):
    #print("Starting to read %s" % sec_name)
    session = boto3.session.Session()
    s3 = session.resource('s3')
    bucket = s3.Bucket('cm-engineers')
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, col]))
    pstr = bucket.Object(path).get().get('Body').read()
    queue.put(path)
    #print("Done reading %s" % sec_name)
    pass

def read_threaded3(queue, sec_name, contract_name=None, category='DAILY', col='close'):
    response = boto3.session.Session().client('lambda').invoke(FunctionName='pdr_price_data_reader')
    if ('StatusCode' not in response) or (int(response['StatusCode']) != 200):
        queue.put("Error")
    else:
        body_stream = response['Payload']
        data = body_stream.read()
        queue.put(data)

def read_threaded4(queue, sec_name, contract_name=None, category='DAILY', col='close'):
    payload = '"%s"' % sec_name
    response = boto3.session.Session().client('lambda').invoke(
        FunctionName='pdr_price_data_sec_reader', Payload=payload)
    if ('StatusCode' not in response) or (int(response['StatusCode']) != 200):
        queue.put("Error")
    else:
        body_stream = response['Payload']
        data = body_stream.read()
        print(len(data))
        if len(data) < 1000:
            print data
        queue.put(data)

def read_threaded5(queue, sec_name, contract_name=None, category='DAILY', col='close'):
    payload = '"%s"' % sec_name
    response = boto3.session.Session().client('lambda').invoke(
        FunctionName='pdr_price_data_thread_reader', Payload=payload)
    if ('StatusCode' not in response) or (int(response['StatusCode']) != 200):
        queue.put("Error")
    else:
        body_stream = response['Payload']
        data = body_stream.read()
        print(len(data))
        if len(data) < 1000:
            print data
        queue.put(data)

def read_threaded6(queue, sec_name, contract_name=None, category='DAILY', col='close'):
    payload = '"%s"' % sec_name
    response = boto3.session.Session().client('lambda').invoke(
        FunctionName='pdr_price_data_global_reader', Payload=payload)
    if ('StatusCode' not in response) or (int(response['StatusCode']) != 200):
        queue.put("Error")
    else:
        body_stream = response['Payload']
        data = body_stream.read()
        print(len(data))
        if len(data) < 1000:
            print data
        queue.put(data)

THREADED = True
ITERATIONS = 3

def __main():
    millis = current_millis()
    if not THREADED:
        global BUCKET
        s3 = boto3.resource('s3')
        BUCKET = s3.Bucket('cm-engineers')

    if DO_WRITES:
        for sec_name in COMDTYS:
            load_symbol(sec_name)

        for maquila_key in MAQUILAS:
            load_maquila(maquila_key, BACKTEST_INDEX)
    elif THREADED:
        q = Queue()
        left_to_process = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            for i in range(ITERATIONS):
                for sec_name in COMDTYS:
                    print(sec_name)
                    executor.submit(read_threaded6, q, sec_name)
                    left_to_process += 1

            print("Done submitting")
            while left_to_process > 0:
                if q.empty():
                    sleep(0.1)
                else:
                    q.get()
                    left_to_process -= 1
    else:
        q = Queue()
        for i in range(ITERATIONS):
            for sec_name in COMDTYS:
                print(sec_name)
                read_threaded6(q, sec_name)
                q.get()
        print("Done submitting")

    # else:
        # read_symbol(sec_name)
        # read_symbol_big(sec_name)
        # read_blob(sec_name, None, None, 'metadata')   # to prime the timings
        # for maquila_key in MAQUILAS:
        #     read_maquila(maquila_key)
        # for sec_name in COMDTYS:
            # read_maquila(sec_name)

    log_millis(millis, "Time to run: ")


if __name__ == '__main__':
    __main()
