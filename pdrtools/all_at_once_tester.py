#! /usr/bin/env python

import datetime
import pytz
import time
from collections import OrderedDict

import boto3
import numpy
import pandas
from pandas.tseries import offsets

from instrumentz import security as isecurity
from pandaux import indaux

current_millis = lambda: int(round(time.time() * 1000))

BUCKET = None

BDAYS_PER_CONTRACT = 150

BACKTEST_START = '1990-01-06'
BACKTEST_END = str(datetime.date.today())
BACKTEST_INDEX = pandas.period_range(start=BACKTEST_START, end=BACKTEST_END, freq='B')
DEEP_PAST = BACKTEST_INDEX[0]

COMDTYS = (
    "HG_COMDTY",
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

METADATA = dict()


def __main():
    millis = current_millis()

    global BUCKET, METADATA
    s3 = boto3.resource('s3')
    BUCKET = s3.Bucket('cm-engineers')

    for sec_name in COMDTYS:
        process_symbol(sec_name)

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

    # Gather start/end stats for all relevant contracts
    contracts_info = OrderedDict()
    for contract_name in df.index:
        if contract_name in BAD_CONTRACTS:
            continue

        METADATA[contract_name] = sec_name

        expiry = df.loc[contract_name, 'expiration']

        end_p = indaux.apply_offset(expiry, offsets.BDay())
        if end_p <= DEEP_PAST:
            continue

        # NOTE: Use inception - 1 BDay if wanting to store all contract data in the future.
        # inception = df.loc[contract_name, 'inception']
        # For now, we're just storing the last BDAYS_PER_CONTRACT business days of prices.
        start_p = max(indaux.apply_offset(expiry, -BDAYS_PER_CONTRACT * offsets.BDay()), DEEP_PAST)

        # Convert to timezone-aware timestamps and naive datetimes (for efficient use elsewhere)
        start_ts = start_p.to_timestamp().tz_localize(pytz.UTC)
        end_ts = end_p.to_timestamp().tz_localize(pytz.UTC)
        start_dt = start_p.to_timestamp().to_datetime()
        end_dt = end_p.to_timestamp().to_datetime()
        contracts_info[contract_name] = (start_ts, end_ts, start_dt, end_dt)

    get_all_intraday_data(instrument, contracts_info)


def get_all_intraday_data(instrument, contracts_info):
    def _convert_to_mins(p):
        tup = p.timetuple()
        return (tup.tm_hour * 60) + tup.tm_min

    def _filter((ts, contract_name)):
        if contract_name in contracts_info:
            # Entries 0 and 1 are start_ts and end_ts
            info = contracts_info[contract_name]
            return (ts >= info[0]) and (ts <= info[1])

    millis = current_millis()
    all_minutes = pandas.period_range(BACKTEST_START, BACKTEST_END, freq="min")
    all_vwaps_df = instrument.get_vwap_data_df(all_minutes, columns=['symbol', 'value', 'volume'])
    all_vwaps_df.index.name = 'date'
    all_vwaps_df.columns = ['contract', 'value', 'volume']
    millis = log_millis(millis, "Time to read all VWAP data: ")
    all_vwaps_df.set_index('contract', append=True, inplace=True, drop=True)
    millis = log_millis(millis, "set_index: ")
    print(len(all_vwaps_df))
    all_vwaps_df = all_vwaps_df.select(_filter, axis=0)
    print(len(all_vwaps_df))
    millis = log_millis(millis, "select: ")
    all_vwaps_df = all_vwaps_df.reset_index()
    millis = log_millis(millis, "reset_index: ")
    # Add columns for the number of minutes into each day, and the date with no time component
    all_vwaps_df.loc[:, 'mins'] = [_convert_to_mins(p) for p in all_vwaps_df.date]
    millis = log_millis(millis, "mins: ")
    all_vwaps_df.loc[:, 'log_s'] = [dt.date() for dt in all_vwaps_df.date]
    millis = log_millis(millis, "log_s: ")
    for mins in INTRADAY_MIN_SPECS:
        grp_name = group_name(mins)
        all_vwaps_df.loc[:, grp_name] = all_vwaps_df.mins // mins
        millis = log_millis(millis, grp_name + ': ')
    all_vwaps_df.drop('date', axis=1, inplace=True)
    millis = log_millis(millis, "drop: ")
    #all_vwaps_df.set_index(['contract', 'log_s', 'mins'], inplace=True)
    #millis = log_millis(millis, "set_index: ")
    #millis = log_millis(millis, "Time to massage VWAP data: ")

    all_vwaps_df['numer'] = all_vwaps_df.value * all_vwaps_df.volume
    millis = log_millis(millis, "numer: ")

    for mins in INTRADAY_MIN_SPECS:
        grouped = all_vwaps_df.groupby([group_name(mins), 'contract', 'log_s'])
        millis = log_millis(millis, "grouped: ")
        mins_df = grouped.agg(OrderedDict([('value', numpy.mean), ('volume', numpy.sum), ('numer', numpy.sum)]))
        mins_df.columns = ['twap', 'volume', 'vwap']
        millis = log_millis(millis, "agg: ")
        mins_df.vwap = mins_df.vwap / mins_df.volume
        millis = log_millis(millis, "vwap: ")
        #print(mins_df)


if __name__ == '__main__':
    __main()
