#! /usr/bin/env python

"""
price_data_updater is a script to incrementally update the prices in the backtest price data store.

The standard use is to call it with no options, which will update all the prices and metadata through 'yester-bday'.

Usage:
  price_data_updater.py [options]

Options:
  --bucket BUCKET         The S3 bucket of the price data store [default: cm-engineers]
  --prefix PREFIX         The prefix for entries in the data store [default: pdr/blobz]

  --symbols SYMBOLS       A list of symbols to process, colon-separated, defaulting to all

  --start-date DATE       Sets the earliest date for price data [default: 1990-01-03]
  --end-date DATE         Sets the latest date for price data, defaulting to 'yester-bday'

  --contract-bdays BDAYS  The default number of business days per contract [default: 290]
  --correction-days DAYS  The default number of days of lookback for corrections [default: 35]
  --granularities GRANS   The default granularities for intraday prices, colon-separated [default: 15:120]

  --skip-daily            Skip daily (close) price processing
  --skip-intraday         Skip intraday (vwap/twap) price processing

  --dry-run               Indicate what would be done, without making any modifications

Examples:
  price_data_updater.py
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import os
import pytz
import sys
import time
from collections import OrderedDict

import boto3
import msgpack
import numpy
import pandas
from pandas.tseries import offsets

import fimc
from envopt import envopt
from instrumentz import contract as icontract
from instrumentz import security as isecurity
from instrumentz import series as iseries
from pandaux import indaux

from price_data import storage as price


current_millis = lambda: int(round(time.time() * 1000))

MINS_IN_DAY = 24 * 60
SECONDS_IN_DAY = MINS_IN_DAY * 60

# Global to hold the global metadata for the price store
METADATA = None  # type: price.Metadata

# A list of known bad contracts
# TODO Should this be part of the metadata, too?
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

# These globals get set by the processing of command-line arguments

BUCKET = None
PREFIX = None

DO_WRITES = True
DO_DAILY = True
DO_INTRADAY = True

BACKTEST_START = None
BACKTEST_END = None
BACKTEST_INDEX = None  # type: pandas.PeriodIndex

# An optional list of commodities to restrict processing to
SYMBOLS = None  # type: tuple

# The list that was used historically
# TODO Eventually remove this.  It is documentary only.
ORIG_SYMBOLS = (
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

# TODO Also documentary and eventually to be removed
OTHER_SYMBOLS = "A5_INDEX:AD_CURNCY:AI_INDEX:AJ_INDEX:ATT_INDEX:AX_INDEX:BC_INDEX:BE_INDEX:" \
              "BP_CURNCY:BZ_INDEX:CD_CURNCY:CF_INDEX:CN_COMDTY:" \
              "C_COMDTY:DU_COMDTY:DW_COMDTY:DX_CURNCY:EC_CURNCY:EO_INDEX:FN_COMDTY:" \
              "FT_INDEX:FV_COMDTY:GI_INDEX:GX_INDEX:HU_COMDTY:HU_INDEX:IB_COMDTY:" \
              "IB_INDEX:IH_INDEX:IJ_COMDTY:IS_INDEX:JB_COMDTY:JO_COMDTY:JY_CURNCY:KAA_COMDTY:KM_INDEX:" \
              "KO_COMDTY:KRS_INDEX:LT_COMDTY:LW_COMDTY:" \
              "NH_INDEX:NQ_INDEX:NX_COMDTY:NX_INDEX:OI_INDEX:OQA_COMDTY:OT_INDEX:PE_CURNCY:" \
              "PP_INDEX:PT_INDEX:QC_INDEX:QW_COMDTY:QZ_INDEX:SF_COMDTY:SF_CURNCY:" \
              "SI_COMDTY:SM_INDEX:SP_INDEX:ST_INDEX:TA_INDEX:TP_INDEX:TU_COMDTY:TW_INDEX:" \
              "UO_INDEX:US_COMDTY:UX_INDEX:VE_INDEX:VG_INDEX:XP_INDEX:XU_INDEX:XW_COMDTY:Z_INDEX"
ALL_SYMBOLS = "A5_INDEX:AA_COMDTY:AD_CURNCY:AI_INDEX:AJ_INDEX:ATT_INDEX:AX_INDEX:BC_INDEX:BE_INDEX:BO_COMDTY:" \
              "BP_CURNCY:BZA_COMDTY:BZ_INDEX:CC_COMDTY:CD_CURNCY:CF_INDEX:CL_COMDTY:CN_COMDTY:CO_COMDTY:CT_COMDTY:" \
              "CU_COMDTY:C_COMDTY:DU_COMDTY:DW_COMDTY:DX_CURNCY:EC_CURNCY:EN_COMDTY:EO_INDEX:ES_INDEX:FN_COMDTY:" \
              "FT_INDEX:FV_COMDTY:GC_COMDTY:GI_INDEX:GX_INDEX:HG_COMDTY:HO_COMDTY:HU_COMDTY:HU_INDEX:IB_COMDTY:" \
              "IB_INDEX:IH_INDEX:IJ_COMDTY:IS_INDEX:JB_COMDTY:JO_COMDTY:JY_CURNCY:KAA_COMDTY:KC_COMDTY:KM_INDEX:" \
              "KO_COMDTY:KRS_INDEX:LA_COMDTY:LC_COMDTY:LH_COMDTY:LL_COMDTY:LN_COMDTY:LP_COMDTY:LT_COMDTY:LW_COMDTY:" \
              "LX_COMDTY:NG_COMDTY:NH_INDEX:NQ_INDEX:NX_COMDTY:NX_INDEX:OI_INDEX:OQA_COMDTY:OT_INDEX:PE_CURNCY:" \
              "PL_COMDTY:PP_INDEX:PT_INDEX:QC_INDEX:QS_COMDTY:QW_COMDTY:QZ_INDEX:SB_COMDTY:SF_COMDTY:SF_CURNCY:" \
              "SI_COMDTY:SM_COMDTY:SM_INDEX:SP_INDEX:ST_INDEX:S_COMDTY:TA_INDEX:TP_INDEX:TU_COMDTY:TW_INDEX:" \
              "TY_COMDTY:UO_INDEX:US_COMDTY:UX_INDEX:VE_INDEX:VG_INDEX:W_COMDTY:XB_COMDTY:XP_INDEX:XU_INDEX:" \
              "XW_COMDTY:Z_INDEX"


def __main():
    millis = current_millis()

    args = envopt(__doc__, env_prefix='PDU')

    process_args(args)

    load_metadata(args)

    process_all_symbols(BACKTEST_END)

    save_metadata()

    log_millis(millis, "Time to run: ")


def log_millis(millis, pattern):
    delta = current_millis() - millis
    print(pattern + str(delta))
    return current_millis()


def process_args(args):
    global BUCKET, PREFIX, DO_WRITES, DO_DAILY, DO_INTRADAY, BACKTEST_START, BACKTEST_END, BACKTEST_INDEX, SYMBOLS

    bucket = args['--bucket']
    BUCKET = boto3.resource('s3').Bucket(bucket)
    PREFIX = args['--prefix']
    print("Using datastore under s3://%s/%s" % (bucket, PREFIX))

    DO_WRITES = not args['--dry-run']
    if not DO_WRITES:
        print("DRY RUN!  Skipping all writing")

    DO_DAILY = not args['--skip-daily']
    if not DO_DAILY:
        print("SKIPPING DAILY PRICES!  Won't update 'current_through' dates in metadata.")

    DO_INTRADAY = not args['--skip-intraday']
    if not DO_INTRADAY:
        print("SKIPPING INTRADAY PRICES!  Won't update 'current_through' dates in metadata.")

    BACKTEST_START = args['--start-date']
    BACKTEST_END = args['--end-date']
    if BACKTEST_END is None:
        BACKTEST_END = str(pandas.Period(datetime.date.today(), freq='B') - 1)
    BACKTEST_INDEX = pandas.period_range(start=BACKTEST_START, end=BACKTEST_END, freq='B')
    print("Processing prices from %s through %s" % (BACKTEST_INDEX[0], BACKTEST_INDEX[-1]))

    symbols = args['--symbols']
    if symbols is not None:
        SYMBOLS = tuple(symbols.split(':'))
        print("Restricting symbols to %s" % (SYMBOLS,))


def load_metadata(args):
    global METADATA

    # noinspection PyBroadException
    try:
        # Initialize contract map with what we've seen before
        METADATA = price.Metadata(read_blob(None, None, price.PriceData.METADATA))
    except Exception:
        print("CREATING EMPTY METADATA OBJECT!")
        METADATA = price.Metadata()

    # Fill in some defaults, if missing
    if METADATA.intraday_granularities(None) is None:
        granularities = tuple(int(x) for x in args['--granularities'].split(':'))
        METADATA.intraday_granularities(None, granularities)
    if METADATA.bdays_per_contract(None) is None:
        METADATA.bdays_per_contract(None, int(args['--contract-bdays']))
    if METADATA.correction_days(None) is None:
        METADATA.correction_days(None, int(args['--correction-days']))


def save_metadata():
    write_blob(None, None, price.PriceData.METADATA, METADATA)


def process_all_symbols(through_date):
    if SYMBOLS is not None:
        symbols = SYMBOLS
    else:
        fimc_provider = fimc.get_provider()
        all_secs = fimc_provider.securities
        symbols = sorted([security for security in all_secs
                          if fimc_provider.metadata(security)['security_type'].startswith('Future')])

    for sec_name in symbols:
        process_symbol(sec_name, through_date)
        save_metadata()


def group_name(mins):
    return 'grp_%d' % mins


def process_symbol(sec_name, through_date):
    if METADATA.blacklist(sec_name):
        print("Skipping %s because it is blacklisted" % sec_name)
        return

    current_through = METADATA.current_through(sec_name)
    if current_through and (current_through >= through_date):
        print("Skipping %s because it is already current through %s" % (sec_name, current_through))
        return

    print("Processing %s" % sec_name)
    millis = current_millis()
    instrument = isecurity.factory(sec_name)

    md_dict = dict()
    for key in instrument.metadata.keys():
        md_dict[key] = instrument.metadata[key]
    write_blob(sec_name, None, price.PriceData.METADATA, md_dict)

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

    # If doing an update, find the threshold that a contract needs to expire on or after to be relevant
    threshold_p = BACKTEST_INDEX[0]  # type: pandas.Period
    if current_through:
        correction_p = (pandas.Period(datetime.date.today(), freq='D') - METADATA.correction_days(sec_name)).asfreq('B')
        current_through_p = pandas.Period(current_through, freq='B')
        threshold_p = max(threshold_p, min(correction_p, current_through_p))

    # Gather start/end stats for all relevant contracts
    contracts_info = OrderedDict()
    for contract_name in df.index:
        if contract_name in BAD_CONTRACTS:
            continue

        METADATA.contract_map[contract_name] = sec_name

        expiry = df.loc[contract_name, 'expiration']

        # Discard contracts that expire before the beginning of the period
        end_p = indaux.apply_offset(expiry, offsets.BDay())
        if end_p < threshold_p:
            continue

        # NOTE: Use inception - 1 BDay if wanting to store all contract data in the future.
        # inception = df.loc[contract_name, 'inception']
        # For now, we're just storing the last BDAYS_PER_CONTRACT business days of prices.
        start_p = max(BACKTEST_INDEX[0],
                      indaux.apply_offset(expiry, -METADATA.bdays_per_contract(sec_name) * offsets.BDay()))

        # Convert to naive datetimes (for efficient use elsewhere)
        start_dt = start_p.to_timestamp().to_datetime()
        end_dt = end_p.to_timestamp().to_datetime()
        contracts_info[contract_name] = (start_p, end_p, start_dt, end_dt)

    if DO_INTRADAY and check_intraday(sec_name):
        write_intraday_data(sec_name, contracts_info, current_through)

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
        write_daily_data(sec_name, security_close_df, current_through)

    # Only indicate that we are "current" if neither daily or intraday were shut off
    if DO_DAILY and DO_INTRADAY:
        METADATA.current_through(sec_name, through_date)

    log_millis(millis, "Total time: ")


def check_intraday(sec_name):
    if METADATA.intraday_blacklist(sec_name):
        print("Skipping intraday for %s because it is blacklisted" % sec_name)
        return False

    intraday = METADATA.intraday(sec_name)
    if intraday is None:
        # It has never been checked or explicitly negated, so check..
        # TODO Replace with a inquiry to instrumentz or fimc
        INTRADAY_COMDTYS = {'CL_COMDTY', 'CO_COMDTY', 'EN_COMDTY', 'XB_COMDTY', 'HO_COMDTY', 'HG_COMDTY', 'SB_COMDTY'}
        return (sec_name in INTRADAY_COMDTYS)
    else:
        return intraday


# noinspection PyUnresolvedReferences,PyTypeChecker
def write_intraday_data(sec_name, contracts_info, do_update):
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
    for granularity in METADATA.intraday_granularities(sec_name):
        grp_name = group_name(granularity)
        all_vwaps_df.loc[:, grp_name] = posix_mins // granularity
    all_vwaps_df.loc[:, 'log_s'] = posix_dates

    all_vwaps_df['numer'] = all_vwaps_df.value * all_vwaps_df.volume

    for granularity in METADATA.intraday_granularities(sec_name):
        grouped = all_vwaps_df.groupby([group_name(granularity), 'contract', 'log_s'])
        mins_wap_df = grouped.agg(OrderedDict([('value', numpy.mean), ('numer', numpy.sum), ('volume', numpy.sum)]))
        mins_wap_df.columns = ['twap', 'vwap', 'volume']
        mins_wap_df.vwap = mins_wap_df.vwap / mins_wap_df.volume
        mins_wap_df.volume = mins_wap_df.volume[~numpy.isnan(mins_wap_df.volume)].astype(int)
        write_mins_wap_df(sec_name, granularity, mins_wap_df, do_update)

    # Note the presence of intraday data in the metadata
    METADATA.intraday(sec_name, True)

    log_millis(millis, "Time to pricess and write intraday: ")


def get_close_prices(contract_name, start, end):
    index = pandas.period_range(start=start, end=end, freq='B')
    # Get close prices, dropping nans
    try:
        close_prices = iseries.daily_close(icontract.factory(contract_name), index, currency='USD').dropna()
    except AssertionError as exc:
        print("Contract %s appears to not exist in prices" % contract_name)
        return None

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


def write_daily_data(sec_name, df, do_update):
    if do_update:
        dict_ = read_blob(sec_name, 'DAILY', 'close')
        storage = price.PriceDataStorage.from_dict(dict_)
    else:
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


def write_mins_wap_df(sec_name, granularity, mins_wap_df, do_update):
    if (mins_wap_df is None) or (len(mins_wap_df) <= 0):
        print("Empty wap_df for %s/%s" % (sec_name, granularity))
        return

    print("%d min waps" % granularity, sep='', end='')
    mins_wap_df = mins_wap_df.reset_index().set_index('log_s')

    for grp, grp_df in mins_wap_df.groupby(group_name(granularity)):
        full_category = "%s/%s/%s" % ('INTRADAY', granularity, grp)
        if do_update:
            v_storage = price.PriceDataStorage.from_dict(read_blob(sec_name, full_category, "vwap"))
            t_storage = price.PriceDataStorage.from_dict(read_blob(sec_name, full_category, "twap"))
        else:
            v_storage = price.PriceDataStorage(price.PriceData.VWAP, sec_name, start=grp * granularity, duration=granularity)
            t_storage = price.PriceDataStorage(price.PriceData.TWAP, sec_name, start=grp * granularity, duration=granularity)

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

        print('.', sep='', end='')
        write_blob(sec_name, full_category, "vwap", v_storage.to_dict())
        write_blob(sec_name, full_category, "twap", t_storage.to_dict())

    print()


def write_blob(sec_name, category, blob_name, data, do_write=DO_WRITES):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, blob_name]))
    if do_write:
        # print(path)
        mstr = msgpack.packb(data, use_bin_type=True)
        BUCKET.put_object(Key=path, Body=mstr)
    else:
        # print("Skipping %s" % path)
        pass


def read_blob(sec_name, category, blob_name):
    path = os.path.join(*filter(None, [PREFIX, sec_name, category, blob_name]))
    # print("Reading: ", path)
    mstr = BUCKET.Object(path).get().get('Body').read()
    return msgpack.unpackb(mstr, use_list=False)


if __name__ == '__main__':
    __main()
