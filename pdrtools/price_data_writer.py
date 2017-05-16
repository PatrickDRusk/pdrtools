#! /usr/bin/env python

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
PREFIX = 'pdr/blobz2'

BDAYS_PER_CONTRACT = 150

DO_WRITES = True
DO_INTRADAY = False
SHORT_TEST = False

BACKTEST_START = '1990-01-06'
BACKTEST_END = str(datetime.date.today())
BACKTEST_INDEX = pandas.period_range(start=BACKTEST_START, end=BACKTEST_END, freq='B')
DEEP_PAST = BACKTEST_INDEX[0]

COMDTYS = (
    "HG_COMDTY",
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

        all_vwaps_df = all_vwaps_df.reset_index()
        all_vwaps_df.date = [dt.tz_convert(None) for dt in all_vwaps_df.date]
        all_vwaps_df.set_index(['contract', 'date'], inplace=True)
        millis = log_millis(millis, "Time to massage VWAP data: ")

    # This will hold multiple contracts' close prices, to be concatenated later
    close_dfs = list()

    # This will end up as a list of length INTRADAY_MIN_SPECS, of lists with lengths determined by the granularity
    # of the WAP calculations, of dataframes having all of the security's WAPs for one daily time slot.  Phew!
    security_wap_dfs_list = None

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

        contract_close_df = get_close_prices(contract_name, start, end)
        if contract_close_df is not None:
            close_dfs.append(contract_close_df)

        if DO_INTRADAY and (sec_name in INTRADAY_COMDTYS):
            contract_wap_dfs_list = handle_intraday_prices(all_vwaps_df, contract_name, start, end, INTRADAY_MIN_SPECS)
            if contract_wap_dfs_list is not None:
                if security_wap_dfs_list is None:
                    # Initialize with this contract's wap dfs
                    security_wap_dfs_list = contract_wap_dfs_list
                else:
                    # This effectively iterates over the number of granularities in INTRADAY_MINS
                    for contract_wap_dfs, security_wap_dfs in zip(contract_wap_dfs_list, security_wap_dfs_list):
                        if len(contract_wap_dfs) <> len(security_wap_dfs):
                            raise ValueError("Very unexpected difference in lengths!")
                        # Now we have two lists of the same length containing dataframes to concatenate
                        for i, (contract_wap_df, security_wap_df) in enumerate(zip(contract_wap_dfs, security_wap_dfs)):
                            if security_wap_df is None:
                                security_wap_dfs[i] = contract_wap_df
                            else:
                                security_wap_dfs[i] = pandas.concat([security_wap_df, contract_wap_df])
                if SHORT_TEST:
                    break

    if len(close_dfs) > 0:
        security_close_df = pandas.concat(close_dfs)
        # noinspection PyTypeChecker
        write_security_daily(sec_name, security_close_df)

    if DO_INTRADAY and (security_wap_dfs_list is not None):
        write_security_intraday(sec_name, security_wap_dfs_list, INTRADAY_MIN_SPECS)


def get_close_prices(contract_name, start, end):
    index = pandas.period_range(start=start, end=end, freq='B')
    # If there is any close data, construct a dataframe with those rows
    try:
        close_prices = iseries.daily_close(icontract.factory(contract_name), index, currency='USD').dropna()
        if len(close_prices):
            close_df = pandas.DataFrame.from_dict(
                OrderedDict((
                    ('contract', contract_name),
                    ('close', close_prices),
                    )
                )
            )
            close_df.index.name = 'log_s'

            # Remove any zero prices
            close_df = close_df[~(close_df.close == 0.0)]

            return close_df
    except Exception as exc:
        print exc
        return None


def handle_intraday_prices(security_all_vwaps_df, contract_name, start, end, mins_specs):
    def _convert_to_mins(p):
        tup = p.timetuple()
        return (tup.tm_hour * 60) + tup.tm_min

    # Manipulations to get minute-granularity period range perfectly covering the [start-end) time span
    index = pandas.period_range(start=(start.asfreq('D')-1), end=end, freq='min')[1:]
    start = index[0].to_timestamp().to_datetime()
    end = index[-1].to_timestamp().to_datetime()

    try:
        min_waps_df = security_all_vwaps_df.loc[contract_name][start:end]
    except KeyError:
        min_waps_df = None

    if (min_waps_df is None) or (len(min_waps_df) <= 0):
        return None

    # Iterate over the different minute specifications requested
    spec_dfs = list()
    for mins in mins_specs:
        if (24 * 60) % mins:
            raise ValueError("Grouping by %s minutes does not fit evenly into a day" % mins)
        num_groups = ((24 * 60) // mins)
        wap_dfs = [None]*num_groups

        # Assign a group number within the current granularity
        min_waps_df.loc[:, 'grp'] = (min_waps_df.mins // mins).values

        # Group by the groups
        grouping = min_waps_df.loc[:, ('log_s', 'value', 'volume', 'grp')].groupby('grp')
        for grp, grp_df in grouping:
            wap_rows = []
            for dt, dt_df in grp_df.groupby('log_s'):
                vwap = stats.wmean(dt_df['value'], dt_df['volume'])
                twap = dt_df['value'].mean()
                vol = dt_df['volume'].sum()
                if vol >= 0.0:
                    vol = int(vol)
                if not (numpy.isnan(vwap) and numpy.isnan(twap) and numpy.isnan(vol)):
                    wap_rows.append([contract_name, dt, vwap, twap, vol])
            tmp_df = pandas.DataFrame(wap_rows, columns=['contract', 'log_s', 'vwap', 'twap', 'volume'])
            wap_dfs[grp] = tmp_df

        spec_dfs.append(wap_dfs)

    return spec_dfs


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


def write_security_intraday(sec_name, security_wap_dfs_list, min_specs):
    for min_spec, security_wap_dfs in zip(min_specs, security_wap_dfs_list):
        for min_group, security_wap_df in enumerate(security_wap_dfs):
            write_wap_df(sec_name, "INTRADAY", min_spec, min_group, security_wap_df)


def write_wap_df(sec_name, category, min_spec, min_group, wap_df):
    if wap_df is None:
        print("Empty wap_df for %s/%s/%s" % (sec_name, min_spec, min_group))
        return

    def maybe_tuple(foo):
        t = tuple(foo)
        if any(t):
            return t
        else:
            return None

    orig_df = wap_df

    wap_df = orig_df.copy().reset_index().set_index(['log_s'])
    storage = price.PriceDataStorage(price.PriceData.VWAP, sec_name)
    for name, df in wap_df.groupby('contract'):
        df = df.loc[:, ('vwap', 'volume')]
        first_date = df.index[0]
        cindex = pandas.date_range(df.index[0], df.index[-1], freq='D')
        values = df.reindex(cindex).values
        values2 = tuple(maybe_tuple(None if numpy.isnan(f) else f for f in row) for row in values)
        storage.add(name, first_date, values2, ('vwap', 'volume'))
    df_dict = storage.to_dict()
    full_category = "%s/%s/%s" % (category, min_spec, min_group)
    write_blob(sec_name, full_category, "vwap", df_dict)

    wap_df = orig_df.copy().reset_index().set_index(['log_s'])
    storage = price.PriceDataStorage(price.PriceData.TWAP, sec_name)
    for name, df in wap_df.groupby('contract'):
        df = df.loc[:, ('twap',)]
        first_date = df.index[0]
        cindex = pandas.date_range(df.index[0], df.index[-1], freq='D')
        values = df.reindex(cindex).values
        values2 = tuple(None if numpy.isnan(row[0]) else row[0] for row in values)
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
