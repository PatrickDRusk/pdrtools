#! /usr/bin/env python

import datetime
import numbers
from collections import OrderedDict

import numpy
import pandas
from pandas.tseries import offsets

from instrumentz import contract, security, series
from pandaux import indaux

import time

current_millis = lambda: int(round(time.time() * 1000))

BACKTEST_START = '1990-01-03'
BACKTEST_END = str(datetime.date.today())
BACKTEST_INDEX = pandas.period_range(start=BACKTEST_START, end=BACKTEST_END, freq='B')

GUESS_INCEPTION = False

SEVEN_YRS = ((7 * 260) * offsets.BDay())
ALMOST_10_YRS = (((9 * 260) + 220) * offsets.BDay())

DEEP_PAST = BACKTEST_INDEX[0]
TODAY = BACKTEST_INDEX[-1]
DEEP_FUTURE = indaux.apply_offset(TODAY, ALMOST_10_YRS)

COMDTYS = (
    "AA_COMDTY",
    "BO_COMDTY",
    "CC_COMDTY",
    "CL_COMDTY",
    "CN_COMDTY",
    "CO_COMDTY",
    "CT_COMDTY",
    "CU_COMDTY",
    "C_COMDTY",
    "DU_COMDTY",
    "DW_COMDTY",
    "FN_COMDTY",
    "FV_COMDTY",
    "GC_COMDTY",
    "HG_COMDTY",
    "HO_COMDTY",
    "HU_COMDTY",
    "IB_COMDTY",
    "IJ_COMDTY",
    "JB_COMDTY",
    "JO_COMDTY",
    "KAA_COMDTY",
    "KC_COMDTY",
    "KO_COMDTY",
    "LA_COMDTY",
    "LC_COMDTY",
    "LH_COMDTY",
    "LL_COMDTY",
    #"LMAHDS03_COMDTY",
    #"LMCADS03_COMDTY",
    #"LMNIDS03_COMDTY",
    #"LMPBDS03_COMDTY",
    #"LMSNDS03_COMDTY",
    #"LMSNDS15_COMDTY",
    #"LMZSDS03_COMDTY",
    "LN_COMDTY",
    "LP_COMDTY",
    "LT_COMDTY",
    "LW_COMDTY",
    "LX_COMDTY",
    "NG_COMDTY",
    "NX_COMDTY",
    "OQA_COMDTY",
    "PL_COMDTY",
    "QS_COMDTY",
    "QW_COMDTY",
    "SB_COMDTY",
    "SF_COMDTY",
    "SI_COMDTY",
    "SM_COMDTY",
    "S_COMDTY",
    "TU_COMDTY",
    "TY_COMDTY",
    "US_COMDTY",
    "W_COMDTY",
    "XB_COMDTY",
    "XW_COMDTY",
)


def analyze_contract(this_contract, inception, expiry, close, volume):
    if (expiry is None) or (isinstance(expiry, numbers.Number) and numpy.isnan(expiry)):
        print this_contract, "has no expiry date"
        expiry = TODAY
    if (inception is None) or (isinstance(inception, numbers.Number) and numpy.isnan(inception)):
        print this_contract, "has no inception date"
        if GUESS_INCEPTION:
            inception = indaux.apply_offset(expiry, -SEVEN_YRS)
        else:
            inception = DEEP_PAST

    contract_df = pandas.DataFrame.from_dict(
        OrderedDict((
            ('Contract', this_contract),
            ('Inception', inception),
            ('Expiry', expiry),
            ('Close', close),
            ('Volume', volume))
        )
    )
    contract_df.index.name = 'Date'
    contract_df = contract_df.reset_index()

    # Check for zeroes
    inside_df = contract_df[(contract_df.Date >= contract_df.Inception) & (contract_df.Date <= contract_df.Expiry)]
    zeroes_df = inside_df[inside_df.Close == 0.0]
    if len(zeroes_df):
        vols_df = zeroes_df[zeroes_df.Volume > 0.0]
        print "%s has %d zero prices, %d with positive volumes" % (this_contract, len(zeroes_df), len(vols_df))

    # Reduce it to those rows that have prices with dates outside of the (Inception, Expiry) range
    outside_df = contract_df[(contract_df.Date < contract_df.Inception) | (contract_df.Date > contract_df.Expiry)]
    if len(outside_df) <= 0:
        return

    # Find the really old ones indicative of a one/two digit year problem
    too_old_date = indaux.apply_offset(expiry, -ALMOST_10_YRS)
    old_df = outside_df[outside_df.Date <= too_old_date]
    if len(old_df):
        vols_df = old_df[old_df.Volume > 0.0]
        print "%s has %d too old prices, %d with positive volumes" % (this_contract, len(old_df), len(vols_df))

    # Find the really recent ones indicative of a one/two digit year problem
    too_recent_date = indaux.apply_offset(inception, ALMOST_10_YRS)
    recent_df = outside_df[outside_df.Date >= too_recent_date]
    if len(recent_df):
        vols_df = recent_df[recent_df.Volume > 0.0]
        print "%s has %d too recent prices, %d with positive volumes" % (this_contract, len(recent_df), len(vols_df))

    # Get rid of those egregious outliers
    outside_df = outside_df[(outside_df.Date > too_old_date) & (outside_df.Date < too_recent_date)]

    # Report on those before inception date
    pre_df = outside_df[outside_df.Date < outside_df.Inception]
    if len(pre_df):
        vols_df = pre_df[pre_df.Volume > 0.0]
        print "%s has %d prices before inception, %d with positive volumes" % (this_contract, len(pre_df), len(vols_df))

    # Report on those after expiry date
    post_df = outside_df[outside_df.Date > outside_df.Expiry]
    if len(post_df):
        vols_df = post_df[post_df.Volume > 0.0]
        print "%s has %d prices after expiry, %d with positive volumes" % (this_contract, len(post_df), len(vols_df))

CONT = True

def verify_data(symbol, index):
    global CONT
    instrument = security.factory(symbol)

    # Create a frame from the expiry map, which will have contract names as the index
    df = pandas.Series(instrument.metadata['expiry_map'])
    df = df.map(lambda x: pandas.Period(x, freq="B"))
    df = df.to_frame('Expiry')

    # Add a column for the inception dates
    try:
        contract_inception = pandas.Series(instrument.metadata['inception_map'])
        contract_inception = contract_inception.map(lambda x: pandas.Period(x, freq="B"))
        df.loc[:, 'Inception'] = contract_inception
    except:
        print "%s has no inception map" % symbol
        df.loc[:, 'Inception'] = numpy.nan

    # Iterate over the contracts looking for bad prices
    for this_contract in df.index:
        if this_contract == "HOZ99_COMDTY":
            CONT = True
        if not CONT:
            continue
        inception = df.loc[this_contract, 'Inception']
        expiry = df.loc[this_contract, 'Expiry']

        # If there is any close or volume data, construct a dataframe with those rows
        try:
            close = series.daily_close(contract.factory(this_contract), index).dropna()
            volume = series.daily_volume(contract.factory(this_contract), index).dropna()
            if len(close) or len(volume):
                analyze_contract(this_contract, inception, expiry, close, volume)
        except:
            print "%s data could not be read" % this_contract


def __main():
    cont = False
    for symbol in COMDTYS:
        cont = (symbol == "CO_COMDTY")
        if cont:
            verify_data(symbol, BACKTEST_INDEX)


if __name__ == '__main__':
    __main()
