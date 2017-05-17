#! /usr/bin/env python

from __future__ import absolute_import
from __future__ import division

import datetime
import os
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
from yamm import stats

current_millis = lambda: int(round(time.time() * 1000))

BUCKET = None
PREFIX = 'pdr/blobz'

BDAYS_PER_CONTRACT = 150

DO_WRITES = True
DO_DAILY = False
DO_INTRADAY = True

SHORT_TEST = False

BACKTEST_START = '2017-04-10' if SHORT_TEST else '1990-01-06'
BACKTEST_END = str(datetime.date.today())
BACKTEST_INDEX = pandas.period_range(start=BACKTEST_START, end=BACKTEST_END, freq='B')
DEEP_PAST = BACKTEST_INDEX[0]

MINS_IN_DAY = 24 * 60

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

INTRADAY_COMDTYS = {'CL_COMDTY', 'CO_COMDTY', 'XB_COMDTY', 'HO_COMDTY', 'HG_COMDTY', 'SB_COMDTY'}
INTRADAY_MIN_SPECS = (15, 120)

CONTRACT_MAP = dict()


def __main():
    millis = current_millis()

    global BUCKET, CONTRACT_MAP
    s3 = boto3.resource('s3')
    BUCKET = s3.Bucket('cm-engineers')

    try:
        # Initialize contract map with what we've seen before
        CONTRACT_MAP = read_blob(None, None, None, 'contract_map')
    except:
        pass

    for sec_name in COMDTYS:
        process_symbol(sec_name)

    write_blob(None, None, 'contract_map', CONTRACT_MAP)

    log_millis(millis, "Time to run: ")


def log_millis(millis, pattern):
    delta = current_millis() - millis
    print(pattern + str(delta))
    return current_millis()


def group_name(mins):
    return 'grp_%d' % mins


def process_symbol(sec_name):
    print("Processing %s" % sec_name)
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

    # This will hold the entirety of VWAP data, if needed
    all_vwaps_df = None
    wap_dfs_list = None
    if DO_INTRADAY and (sec_name in INTRADAY_COMDTYS):
        def _convert_to_mins(p):
            tup = p.timetuple()
            return (tup.tm_hour * 60) + tup.tm_min

        millis = current_millis()
        all_minutes = pandas.period_range(BACKTEST_START, BACKTEST_END, freq="min")
        all_vwaps_df = instrument.get_vwap_data_df(all_minutes, columns=['symbol', 'value', 'volume'])
        all_vwaps_df.columns = ['contract', 'value', 'volume']
        millis = log_millis(millis, "Time to read all VWAP data: ")

        # Add columns for the number of minutes into each day, and the date with no time component
        all_vwaps_df.loc[:, 'mins'] = [_convert_to_mins(p) for p in all_vwaps_df.index]
        all_vwaps_df.loc[:, 'log_s'] = [dt.date() for dt in all_vwaps_df.index]
        for mins in INTRADAY_MIN_SPECS:
            grp_name = group_name(mins)
            all_vwaps_df.loc[:, grp_name] = all_vwaps_df.mins // mins

        all_vwaps_df = all_vwaps_df.reset_index()
        all_vwaps_df.drop('date', axis=1, inplace=True)
        all_vwaps_df.set_index(['contract', 'log_s', 'mins'], inplace=True)
        millis = log_millis(millis, "Time to massage VWAP data: ")

        # This will end up as a list of length INTRADAY_MIN_SPECS, of lists with lengths determined by the granularity
        # of the WAP calculations, of dataframes having all of the security's WAPs for one daily time slot.  Phew!
        wap_dfs_list = create_wap_dfs_list(INTRADAY_MIN_SPECS)

    # This will hold multiple contracts' close prices, to be concatenated later
    close_dfs = list()

    # Gather the security's data one contract at a time, appending to those variables above
    for contract_name in df.index:
        if contract_name in BAD_CONTRACTS:
            continue

        CONTRACT_MAP[contract_name] = sec_name

        inception = df.loc[contract_name, 'inception']
        expiry = df.loc[contract_name, 'expiration']

        end = indaux.apply_offset(expiry, offsets.BDay())
        if end <= DEEP_PAST:
            continue

        # NOTE: Use inception - 1 BDay if wanting to store all contract data in the future.
        # For now, we're just storing the last BDAYS_PER_CONTRACT business days of prices.
        start = indaux.apply_offset(expiry, -BDAYS_PER_CONTRACT * offsets.BDay())

        if DO_DAILY:
            contract_close_df = get_close_prices(contract_name, start, end)
            if contract_close_df is not None:
                close_dfs.append(contract_close_df)

        if DO_INTRADAY and (sec_name in INTRADAY_COMDTYS):
            get_intraday_prices(all_vwaps_df, contract_name, start, end, wap_dfs_list)

    if DO_DAILY and (len(close_dfs) > 0):
        security_close_df = pandas.concat(close_dfs)
        # noinspection PyTypeChecker
        write_security_daily(sec_name, security_close_df)

    if DO_INTRADAY and (sec_name in INTRADAY_COMDTYS):
        write_security_intraday(sec_name, wap_dfs_list)


def get_close_prices(contract_name, start, end):
    index = pandas.period_range(start=start, end=end, freq='B')
    # If there is any close data, construct a dataframe with those rows
    try:
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
    except Exception as exc:
        print exc

    return None


def create_wap_dfs_list(mins_specs):
    waps_dfs_list = list()
    for mins in mins_specs:
        if (MINS_IN_DAY % mins) <> 0:
            raise ValueError("Grouping by %s minutes does not fit evenly into a day" % mins)
        num_groups = (MINS_IN_DAY // mins)
        wap_dfs = [list() for i in range(num_groups)]
        waps_dfs_list.append(wap_dfs)

    return waps_dfs_list


def get_intraday_prices(security_all_vwaps_df, contract_name, start, end, wap_dfs_list):
    def _convert_to_mins(p):
        tup = p.timetuple()
        return (tup.tm_hour * 60) + tup.tm_min

    # Manipulations to get minute-granularity period range perfectly covering the [start-end) time span
    start = start.to_timestamp().to_datetime()
    end = end.to_timestamp().to_datetime()

    try:
        min_waps_df = security_all_vwaps_df.loc[contract_name][start:end].reset_index()
    except KeyError as exc:
        min_waps_df = None

    if (min_waps_df is None) or (len(min_waps_df) <= 0):
        return None

    # Iterate over the different minute specifications requested
    for wap_dfs in wap_dfs_list:
        # Infer the minutes per bucket from the list
        mins = int(MINS_IN_DAY / len(wap_dfs))
        grp_name = group_name(mins)

        # Group by the groups
        grouping = min_waps_df.loc[:, ('log_s', 'value', 'volume', grp_name)].groupby(grp_name)
        for grp, grp_df in grouping:
            dates = []
            wap_rows = []
            for dt, dt_df in grp_df.groupby('log_s'):
                vwap = stats.wmean(dt_df.value, dt_df.volume)
                twap = dt_df.value.mean()
                vol = dt_df.volume.sum()
                if vol >= 0.0:
                    vol = int(vol)
                if not (numpy.isnan(vwap) and numpy.isnan(twap) and numpy.isnan(vol)):
                    dates.append(dt)
                    wap_rows.append([contract_name, vwap, twap, vol])
            tmp_df = pandas.DataFrame(wap_rows, columns=['contract', 'vwap', 'twap', 'volume'], index=dates)
            tmp_df.index.name = 'log_s'
            wap_dfs[grp].append(tmp_df)


def write_security_daily(sec_name, df):
    storage = price.PriceDataStorage('close', sec_name)
    for name, series in df.close.groupby(df['contract']):
        first_date = series.index[0].to_timestamp().to_datetime().date()
        cindex = pandas.period_range(series.index[0], series.index[-1], freq='D')
        values = series.asfreq('D').reindex(cindex).values.tolist()
        values = [None if numpy.isnan(f) else f for f in values]
        storage.add(name, first_date, values, ('close',))
    df_dict = storage.to_dict()
    write_blob(sec_name, 'DAILY', 'close', df_dict)


def write_security_intraday(sec_name, wap_dfs_list):
    for wap_dfs in wap_dfs_list:
        # Infer the minutes per bucket from the list
        mins = int(MINS_IN_DAY / len(wap_dfs))
        for min_group, min_wap_dfs in enumerate(wap_dfs):
            if len(min_wap_dfs) > 0:
                wap_df = pandas.concat(min_wap_dfs)
                write_wap_df(sec_name, "INTRADAY", mins, min_group, wap_df)


def write_wap_df(sec_name, category, min_spec, min_group, wap_df):
    if (wap_df is None) or (len(wap_df) <= 0):
        print("Empty wap_df for %s/%s/%s" % (sec_name, min_spec, min_group))
        return

    def maybe_tuple(foo):
        t = tuple(foo)
        if any(t):
            return t
        else:
            return None

    orig_df = wap_df

    wap_df = orig_df.copy().loc[:, ('contract', 'vwap', 'volume')]
    storage = price.PriceDataStorage(price.PriceData.VWAP, sec_name)
    for name, df in wap_df.groupby('contract'):
        first_date = df.index[0].to_datetime().date()
        cindex = pandas.date_range(df.index[0], df.index[-1], freq='D')
        values = df.reindex(cindex).values
        values2 = tuple(maybe_tuple(None if numpy.isnan(f) else f for f in row[1:]) for row in values)
        storage.add(name, first_date, values2, ('vwap', 'volume'))
    df_dict = storage.to_dict()
    full_category = "%s/%s/%s" % (category, min_spec, min_group)
    write_blob(sec_name, full_category, "vwap", df_dict)

    wap_df = orig_df.loc[:, ('contract', 'twap')]
    storage = price.PriceDataStorage(price.PriceData.TWAP, sec_name)
    for name, df in wap_df.groupby('contract'):
        first_date = df.index[0].to_datetime().date()
        cindex = pandas.date_range(df.index[0], df.index[-1], freq='D')
        values = df.reindex(cindex).values
        values2 = tuple(None if numpy.isnan(row[1]) else row[1] for row in values)
        storage.add(name, first_date, values2, ('twap',))
    df_dict = storage.to_dict()
    full_category = "%s/%s/%s" % (category, min_spec, min_group)
    write_blob(sec_name, full_category, "twap", df_dict)


def write_blob(sec_name, category, blob_name, data, do_write=DO_WRITES):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, blob_name]))
    if do_write:
        print(path)
        mstr = msgpack.packb(data, use_bin_type=True)
        BUCKET.put_object(Key=path, Body=mstr)
    else:
        print("Skipping blob write")


def read_blob(sec_name, contract_name, category, blob_name):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, contract_name, blob_name]))
    print("Reading: ", path)
    mstr = BUCKET.Object(path).get().get('Body').read()
    return msgpack.unpackb(mstr, use_list=False)


if __name__ == '__main__':
    __main()
