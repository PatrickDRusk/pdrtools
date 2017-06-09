#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import os
import pytz
import time
from collections import OrderedDict

import boto3
import msgpack
import numpy
import pandas
from pandas.tseries import offsets

from instrumentz import contract as icontract
from instrumentz import security as isecurity
from instrumentz import series as iseries
from pandaux import indaux

from price_data import storage as price

current_millis = lambda: int(round(time.time() * 1000))

BUCKET = None

DEFAULT_BDAYS_PER_CONTRACT = 290
DEFAULT_CORRECTION_DAYS = 35
DEFAULT_INTRADAY_GRANULARITIES = (15, 120, )
CURRENT_THROUGH = '2017-06-05'

DEFAULT_BUCKET = 'cm-engineers'
DEFAULT_PREFIX = 'pdr/blobz'

DO_WRITES = False
DO_DAILY = False
DO_INTRADAY = False

SHORT_TEST = False

BACKTEST_START = '2017-03-02' if SHORT_TEST else '1990-01-03'
BACKTEST_END = str(datetime.date.today())
BACKTEST_INDEX = pandas.period_range(start=BACKTEST_START, end=BACKTEST_END, freq='B')
DEEP_PAST = BACKTEST_INDEX[0]

MINS_IN_DAY = 24 * 60
SECONDS_IN_DAY = MINS_IN_DAY * 60

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

FULL_COMDTYS = (
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

INTRADAY_COMDTYS = {'CL_COMDTY', 'CO_COMDTY', 'EN_COMDTY', 'XB_COMDTY', 'HO_COMDTY', 'HG_COMDTY', 'SB_COMDTY'}

METADATA = price.Metadata()


def __main():
    millis = current_millis()

    global BUCKET
    BUCKET = boto3.resource('s3').Bucket(DEFAULT_BUCKET)
    print(DEFAULT_BUCKET)
    print(DEFAULT_PREFIX)

    load_metadata()

    for sec_name in COMDTYS:
        process_symbol(sec_name)
        save_metadata()

    save_metadata(do_write=True)

    log_millis(millis, "Time to run: ")


def load_metadata():
    global METADATA
    # noinspection PyBroadException
    try:
        # Initialize contract map with what we've seen before
        METADATA = price.Metadata(read_blob(None, None, None, 'metadata'))
    except Exception:
        METADATA = price.Metadata()

    # Fill in some defaults, if missing
    if METADATA.intraday_granularities(None) is None:
        METADATA.intraday_granularities(None, DEFAULT_INTRADAY_GRANULARITIES)
    if METADATA.bdays_per_contract(None) is None:
        METADATA.bdays_per_contract(None, DEFAULT_BDAYS_PER_CONTRACT)
    if METADATA.correction_days(None) is None:
        METADATA.correction_days(None, DEFAULT_CORRECTION_DAYS)


def log_millis(millis, pattern):
    delta = current_millis() - millis
    print(pattern + str(delta))
    return current_millis()


def save_metadata(do_write=DO_WRITES):
    write_blob(None, None, 'metadata', METADATA, do_write=do_write)


def group_name(mins):
    return 'grp_%d' % mins


def process_symbol(sec_name):
    print("Processing %s" % sec_name)
    millis = current_millis()
    instrument = isecurity.factory(sec_name)

    md_dict = dict()
    for key in instrument.metadata.keys():
        md_dict[key] = instrument.metadata[key]
    write_blob(sec_name, None, 'metadata', md_dict)

    # Create a frame from the expiry map, which will have contract names as the index
    df = pandas.Series(md_dict['expiry_map'])
    df = df.map(lambda x: pandas.Period(x, freq="B"))
    df = df.to_frame('expiration')
    df.index.name = 'contract'

    # Add a column for the inception dates
    # NOTE: Keeping this in case we want to write all contract pricess as separate blobs in the future
    inceptions = md_dict.get('inception_map', None)
    if inceptions is not None:
        contract_inception = pandas.Series(inceptions)
        contract_inception = contract_inception.map(lambda x: pandas.Period(x, freq="B"))
        df.loc[:, 'inception'] = contract_inception
    else:
        df.loc[:, 'inception'] = None

    METADATA.current_through(sec_name, CURRENT_THROUGH)
    if (sec_name in INTRADAY_COMDTYS) and (METADATA.intraday(sec_name) is None):
        METADATA.intraday(sec_name, True)

    # Gather start/end stats for all relevant contracts
    contracts_info = OrderedDict()
    for contract_name in df.index:
        if contract_name in BAD_CONTRACTS:
            continue

        METADATA.contract_map[contract_name] = sec_name

        expiry = df.loc[contract_name, 'expiration']

        end_p = indaux.apply_offset(expiry, offsets.BDay())
        if end_p <= DEEP_PAST:
            continue

        # NOTE: Use inception - 1 BDay if wanting to store all contract data in the future.
        # inception = df.loc[contract_name, 'inception']
        # For now, we're just storing the last BDAYS_PER_CONTRACT business days of prices.
        start_p = max(indaux.apply_offset(expiry, -DEFAULT_BDAYS_PER_CONTRACT * offsets.BDay()), DEEP_PAST)

        # Convert to naive datetimes (for efficient use elsewhere)
        start_dt = start_p.to_timestamp().to_datetime()
        end_dt = end_p.to_timestamp().to_datetime()
        contracts_info[contract_name] = (start_p, end_p, start_dt, end_dt)

    if DO_INTRADAY and (sec_name in INTRADAY_COMDTYS):
        write_all_intraday_data(sec_name, contracts_info)

    # This will hold multiple contracts' close prices, to be concatenated later
    close_dfs = list()

    # Gather the security's data one contract at a time, appending to those variables above
    for contract_name, (__, __, start_dt, end_dt) in contracts_info.iteritems():
        if DO_DAILY:
            contract_close_df = get_close_prices(contract_name, start_dt, end_dt)
            if contract_close_df is not None:
                close_dfs.append(contract_close_df)

    if DO_DAILY and (len(close_dfs) > 0):
        security_close_df = pandas.concat(close_dfs)
        # noinspection PyTypeChecker
        write_security_daily(sec_name, security_close_df)

    log_millis(millis, "Total time: ")


# noinspection PyUnresolvedReferences,PyTypeChecker
def write_all_intraday_data(sec_name, contracts_info):
    millis = current_millis()

    contract_waps_df_list = list()
    for contract_name, (start_p, end_p, __, __) in contracts_info.iteritems():
        # Manipulations to get minute-granularity period range perfectly covering the [start-end) time span
        index = pandas.period_range(start=(start_p.asfreq('D')-1), end=end_p, freq='min')[1:]

        contract_waps_df = icontract.factory(contract_name).get_vwap_data_df(index, columns=['value', 'volume'], skip_align=True)
        if (contract_waps_df is None) or (len(contract_waps_df) <= 0):
            continue
        contract_waps_df = contract_waps_df[~numpy.isnan(contract_waps_df.value)]
        if (contract_waps_df is None) or (len(contract_waps_df) <= 0):
            continue

        contract_waps_df['contract'] = contract_name
        contract_waps_df_list.append(contract_waps_df)

    all_vwaps_df = pandas.concat(contract_waps_df_list)
    all_vwaps_df.index.name = 'date'
    millis = log_millis(millis, "Time to read %s rows of VWAP data: " % len(all_vwaps_df))
    posix = all_vwaps_df.index.astype(numpy.int64) // 1000000000
    posix_seconds = posix % SECONDS_IN_DAY
    posix_dates = posix - posix_seconds
    posix_mins = posix_seconds // 60
    for mins in DEFAULT_INTRADAY_GRANULARITIES:
        grp_name = group_name(mins)
        all_vwaps_df.loc[:, grp_name] = posix_mins // mins
    all_vwaps_df.loc[:, 'log_s'] = posix_dates

    all_vwaps_df['numer'] = all_vwaps_df.value * all_vwaps_df.volume

    for mins in DEFAULT_INTRADAY_GRANULARITIES:
        grouped = all_vwaps_df.groupby([group_name(mins), 'contract', 'log_s'])
        mins_wap_df = grouped.agg(OrderedDict([('value', numpy.mean), ('numer', numpy.sum), ('volume', numpy.sum)]))
        mins_wap_df.columns = ['twap', 'vwap', 'volume']
        mins_wap_df.vwap = mins_wap_df.vwap / mins_wap_df.volume
        mins_wap_df.volume = mins_wap_df.volume[~numpy.isnan(mins_wap_df.volume)].astype(int)
        write_mins_wap_df(sec_name, mins, mins_wap_df)

    log_millis(millis, "Time to pricess and write intraday: ")


def get_close_prices(contract_name, start, end):
    index = pandas.period_range(start=start, end=end, freq='B')
    # Get close prices, dropping nans
    close_prices = iseries.daily_close(icontract.factory(contract_name), index, currency='USD').dropna()
    # Remove any zero prices
    close_prices = close_prices[~(close_prices == 0.0)]

    if len(close_prices):
        close_df = pandas.DataFrame.from_dict(
            OrderedDict((
                ('contract', contract_name),
                ('close', close_prices),
                )
            )
        )
        close_df.index.name = 'log_s'
        return close_df

    return None


def write_security_daily(sec_name, df):
    storage = price.PriceDataStorage('close', sec_name)
    for name, series in df.close.groupby(df['contract']):
        first_date = series.index[0].to_timestamp().to_datetime().date()
        cindex = pandas.period_range(series.index[0], series.index[-1], freq='D')
        values = series.asfreq('D').reindex(cindex).values.tolist()
        values = (tuple(None if numpy.isnan(f) else f for f in values), )
        storage.add(name, first_date, values, ('close',))
    df_dict = storage.to_dict()
    print("close")
    write_blob(sec_name, 'DAILY', 'close', df_dict)


def write_mins_wap_df(sec_name, mins, mins_wap_df):
    if (mins_wap_df is None) or (len(mins_wap_df) <= 0):
        print("Empty wap_df for %s/%s" % (sec_name, mins))
        return

    print("%d min waps" % mins, sep='', end='')
    mins_wap_df = mins_wap_df.reset_index().set_index('log_s')

    for grp, grp_df in mins_wap_df.groupby(group_name(mins)):
        v_storage = price.PriceDataStorage(price.PriceData.VWAP, sec_name, start=grp*mins, duration=mins)
        t_storage = price.PriceDataStorage(price.PriceData.TWAP, sec_name, start=grp*mins, duration=mins)

        for contract_name, contract_df in grp_df.groupby('contract'):
            first_posix_date = contract_df.index[0]
            last_posix_date = contract_df.index[-1]
            if first_posix_date < last_posix_date:
                num_days = ((last_posix_date - first_posix_date) / SECONDS_IN_DAY) + 1
                new_index = numpy.linspace(first_posix_date, last_posix_date, num=num_days)
                contract_df = contract_df.reindex(new_index)
            values = contract_df.values
            # rows in values look like [grp, contract_name, twap, vwap, volume]
            v_values = (tuple(None if numpy.isnan(row[3]) else row[3] for row in values),
                        tuple(None if numpy.isnan(row[4]) else row[4] for row in values))
            t_values = (tuple(None if numpy.isnan(row[2]) else row[2] for row in values), )

            first_date = datetime.datetime.fromtimestamp(first_posix_date, pytz.UTC).date()
            v_storage.add(contract_name, first_date, v_values, ('vwap', 'volume'))
            t_storage.add(contract_name, first_date, t_values, ('twap',))

        full_category = "%s/%s/%s" % ('INTRADAY', mins, grp)
        print('.', sep='', end='')
        write_blob(sec_name, full_category, "vwap", v_storage.to_dict())
        write_blob(sec_name, full_category, "twap", t_storage.to_dict())

    print()


def write_blob(sec_name, category, blob_name, data, do_write=DO_WRITES):
    path = os.path.join(*filter(None, [DEFAULT_PREFIX, sec_name, category, blob_name]))
    if do_write:
        # print(path)
        mstr = msgpack.packb(data, use_bin_type=True)
        BUCKET.put_object(Key=path, Body=mstr)
    else:
        # print("Skipping %s" % path)
        pass


def read_blob(sec_name, contract_name, category, blob_name):
    path = os.path.join(*filter(None, [DEFAULT_PREFIX, sec_name, category, contract_name, blob_name]))
    # print("Reading: ", path)
    mstr = BUCKET.Object(path).get().get('Body').read()
    return msgpack.unpackb(mstr, use_list=False)


if __name__ == '__main__':
    __main()
