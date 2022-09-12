import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import time
import os
import numpy as np
from Mint import *
from DataCleaning import *

#The LP.py file is used to calculate LP returns for Uniswap V2 and Uniswap V3

#Returns are calculated similarly as in "Can Markets be Fully Automated?
# Evidence From an “Automated Market Maker” " by Peter O'Neill for V3, but more realistic assumptions are made
#Returns are calculated similarly as in "When Uniswap v3 returns more fees for passive LPs", by Austin Adams Gordon Liao
# for V2

#Returns for V2 and V3 are split into components as in "Can Markets be Fully Automated?
# Evidence From an “Automated Market Maker” " by Peter O'Neill

### LP returns V3 ####


def LPreturns(Fees, Pools, Burns):
    Fees = Fees
    Pools = Pools
    Burns = Burns

    # Clean Data
    Fees = Fees.sort_values(by=["timestamp"], inplace=False, ascending=False)
    Pools['token0Price'] = Pools['token0Price'].astype(float)
    Pools['token1Price'] = Pools['token1Price'].astype(float)

    # Merge Data
    Fees2 = pd.merge(Fees, Pools[['token1Price', 'token0Price', 'timestamp']], on=['timestamp'], how="inner")

    # Calculate Fees for collectedFeesToken >= withdrawn, else -999 used to filter out odd transactions
    Fees2['FeesToken0'] = np.where(Fees2['collectedFeesToken0'] >= Fees2['withdrawnToken0'],
                                   Fees2['collectedFeesToken0'] - Fees2['withdrawnToken0'], -999)
    Fees2['FeesToken1'] = np.where(Fees2['collectedFeesToken1'] >= Fees2['withdrawnToken1'],
                                   Fees2['collectedFeesToken1'] - Fees2['withdrawnToken1'], -999)

    # Merge Fees dataframe with Burns dataframe to make sure data is correct
    A = pd.merge(Fees2, Burns[['amount0', 'amount1', 'timestamp', 'origin', 'tickLower', 'tickUpper']],
                 on=['origin', 'tickLower', 'tickUpper'], how="inner")

    # Remove positions where withdrawn not equal to amounts from burn file (should be equal)
    B = A[(A['withdrawnToken0'] == -1 * A['amount0']) & (A['withdrawnToken1'] == -1 * A['amount1'])]

    # Remove postions where collectedFeesToken < withdrawn strangely, as collectedFeesToken should equal
    # withdrawn + fees
    C = B[(B['FeesToken0'] != -999.0) & (B['FeesToken1'] != -999.0)]

    Pools2 = Pools.rename(columns={'timestamp': 'timestamp_y'})

    D = pd.merge(C, Pools2[['token1Price', 'token0Price', 'timestamp_y']], on=['timestamp_y'], how="inner")

    #Split returns into components as in paper by Peter O'Neill
    #AdselCos should be negative (almost) everywhere
    #FeeYield should be non-negative
    #InvHolRet can be negative or positive
    R = pd.DataFrame()
    R['V0'] = D['depositedToken0'] + D['depositedToken1']*D['token0Price_x'] #Portfolio Value time 0
    R['VT'] = D['withdrawnToken0'] + D['withdrawnToken1']*D['token0Price_y'] #Portfolio Value time T (burn)
    R['F'] = D['FeesToken0'] + D['FeesToken1']*D['token0Price_y'] #Fixed portfolio Value time T
    R['Tot'] = ( (R['VT'] + R['F']) / R['V0'] -1 ) #* 100 #Total Returns

    R['InvHolRet'] = ( (D['depositedToken0'] + D['depositedToken1']*D['token0Price_y'])/R['V0']) - 1 #Inventory Holding Returns
    R['AdSelCos'] = (R['VT']/R['V0'] - 1) - R['InvHolRet'] #Adverse Selection Costs
    R['FeeYield'] = R['F']/R['V0'] #Fee Yield

    R['Tot2'] = R['InvHolRet'] + R['AdSelCos'] + R['FeeYield'] # Check total returns

    return R, D

def LPreturnsV2(LiqSnaps, burnsV2, mintsV2):
    LiqSnaps = LiqSnaps
    mintsV2 = mintsV2
    burnsV2 = burnsV2

    # Clean Data
    LiqSnaps = LiqSnaps.rename(columns={'user.id': 'origin'})
    LiqSnaps['reserve0'] = LiqSnaps['reserve0'].astype(float)
    LiqSnaps['reserve1'] = LiqSnaps['reserve1'].astype(float)
    mintsV2 = mintsV2.rename(columns={'to': 'origin'})
    burnsV2 = burnsV2.rename(columns={'sender': 'origin'})
    mintsV2['amount0'] = mintsV2['amount0'].astype(float)
    mintsV2['amount1'] = mintsV2['amount1'].astype(float)
    burnsV2['amount0'] = burnsV2['amount0'].astype(float)
    burnsV2['amount1'] = burnsV2['amount1'].astype(float)
    A = pd.merge(mintsV2, burnsV2, on=['liquidity'], how="inner")
    A['timestamp_x'] = A['timestamp_x'].astype(int)
    A['timestamp_y'] = A['timestamp_y'].astype(int)

    C = pd.merge(A, LiqSnaps[['timestamp', 'reserve0', 'reserve1', 'liquidityTokenTotalSupply']], left_on='timestamp_x',
                 right_on='timestamp').drop('timestamp', axis=1)
    B = pd.merge(C, LiqSnaps[['timestamp', 'reserve0', 'reserve1', 'liquidityTokenTotalSupply']], left_on='timestamp_y',
                 right_on='timestamp').drop('timestamp', axis=1)
    B = B.drop_duplicates(keep='first', subset=['origin_x', 'timestamp_x', 'liquidity'])

    # Calculate variables of interest for V2 Returns
    B['P_x'] = B['reserve1_x'] / B['reserve0_x']
    B['P_y'] = B['reserve1_y'] / B['reserve0_y']
    B['liquidityTokenTotalSupply_x'] = B['liquidityTokenTotalSupply_x'].astype(float)
    B['liquidityTokenTotalSupply_y'] = B['liquidityTokenTotalSupply_y'].astype(float)
    B['liquidity'] = B['liquidity'].astype(float)

    R = pd.DataFrame()
    R['s_x'] = B['liquidity'] / B['liquidityTokenTotalSupply_x']
    R['s_y'] = B['liquidity'] / B['liquidityTokenTotalSupply_y']

    # Code mainly from "When Uniswap v3 returns more fees for passive LPs", by Austin Adams Gordon Liao for V2
    v2_pool = pd.DataFrame()
    v2_pool["tok1/tok0_t1"] = B["reserve0_y"].astype(float) / B["reserve1_y"].astype(float)
    v2_pool["tok1/tok0_t0"] = B["reserve0_x"].astype(float) / B["reserve1_x"].astype(float)
    v2_pool["totalSupply"] = B["liquidityTokenTotalSupply_y"].astype(float)
    v2_pool["totalSupply_t0"] = B["liquidityTokenTotalSupply_x"].astype(float)
    v2_pool["pctSupplied"] = R['s_x']

    v2_pool["reserve0"] = B["reserve0_y"].astype(float)
    v2_pool["reserve0_t0"] = B["reserve0_x"]

    v2_pool["reserve1"] = B["reserve1_y"].astype(float)
    v2_pool["reserve1_t0"] = B["reserve1_x"]

    v2_pool["tok0_t0"] = v2_pool["pctSupplied"] * v2_pool["reserve0_t0"]
    v2_pool["tok1_t0"] = v2_pool["pctSupplied"] * v2_pool["reserve1_t0"]

    v2_pool["k_t0"] = v2_pool["reserve0_t0"] * v2_pool["reserve1_t0"]
    v2_pool["x_t0"] = (np.sqrt(v2_pool["k_t0"] * v2_pool["tok1/tok0_t0"]) * v2_pool["pctSupplied"])
    v2_pool["y_t0"] = (np.sqrt(v2_pool["k_t0"] / v2_pool["tok1/tok0_t0"]) * v2_pool["pctSupplied"])
    v2_pool["x_t1"] = (np.sqrt(v2_pool["k_t0"] * v2_pool["tok1/tok0_t1"]) * v2_pool["pctSupplied"])
    v2_pool["y_t1"] = (np.sqrt(v2_pool["k_t0"] / v2_pool["tok1/tok0_t1"]) * v2_pool["pctSupplied"])

    v2_pool["synPort_t0"] = v2_pool["x_t0"] + v2_pool["y_t0"] * v2_pool["tok1/tok0_t0"]
    v2_pool["synPort_t1"] = v2_pool["x_t1"] + v2_pool["y_t1"] * v2_pool["tok1/tok0_t1"]

    v2_pool["v2Port_t0"] = (v2_pool["pctSupplied"] * v2_pool["reserve0_t0"]) + (
                v2_pool["pctSupplied"] * v2_pool["reserve1_t0"] * v2_pool["tok1/tok0_t0"])

    v2_pool["hodlPort_t1-t0"] = (v2_pool["pctSupplied"] * v2_pool["reserve0_t0"]) + (
                v2_pool["pctSupplied"] * v2_pool["reserve1_t0"] * v2_pool["tok1/tok0_t1"])

    v2_pool["v2Port_t1"] = (
                                   ((v2_pool["pctSupplied"] * v2_pool["totalSupply_t0"]) / v2_pool["totalSupply"])
                                   * v2_pool["reserve0"]
                           ) + (
                                   ((v2_pool["pctSupplied"] * v2_pool["totalSupply_t0"]) / v2_pool["totalSupply"])
                                   * v2_pool["reserve1"]
                                   * v2_pool["tok1/tok0_t1"]
                           )

    v2_pool["feeRet"] = v2_pool["v2Port_t1"] - v2_pool["synPort_t1"]
    v2_pool["pctRet"] = v2_pool["feeRet"] / v2_pool["v2Port_t0"]
    #

    #Split returns into components as in paper by Peter O'Neill
    #AdselCos should be negative (almost) everywhere
    #FeeYield should be non-negative
    #InvHolRet can be negative or positive
    R['V0'] = (B['reserve0_x'] * B['P_x'] + B['reserve1_x']) * R['s_x'] #Portfolio Value time 0
    R['VT'] = (B['reserve0_y'] * B['P_y'] + B['reserve1_y']) * R['s_y'] #Portfolio Value time T (burn)
    R['VT,FIX'] = (B['reserve0_x'] * B['P_y'] + B['reserve1_x']) * R['s_x'] #Fixed Portfolio Value time T
    R['Tot'] = (B['reserve1_y'] * R['s_y']) / (B['reserve1_x'] * R['s_x']) - 1 # Total Returns
    R['InvHolRet'] = R['VT,FIX'] / R['V0'] - 1 # Inventory Holding Returns
    R['F'] = v2_pool["pctRet"] # Fee Yield
    R['AdSelCos'] = R['Tot'] - R['InvHolRet'] - R['F'] # Adverse Selection Costs

    return R, B