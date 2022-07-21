import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import time
import os
import numpy as np
from Mint import *
from DataCleaning import *


def LPreturns(Fees, Pools, Burns):
    Fees = Fees
    Pools = Pools
    Burns = Burns

    Fees = Fees.sort_values(by=["timestamp"], inplace=False, ascending=False)

    Pools['token0Price'] = Pools['token0Price'].astype(float)
    Pools['token1Price'] = Pools['token1Price'].astype(float)

    Fees2 = pd.merge(Fees, Pools[['token1Price', 'token0Price', 'timestamp']], on = ['timestamp'] , how = "inner")

    Fees2['FeesToken0'] = np.where(Fees2['collectedFeesToken0'] >= Fees2['withdrawnToken0'], Fees2['collectedFeesToken0'] - Fees2['withdrawnToken0'], -999)
    Fees2['FeesToken1'] = np.where(Fees2['collectedFeesToken1'] >= Fees2['withdrawnToken1'], Fees2['collectedFeesToken1'] - Fees2['withdrawnToken1'], -999)

    #Remove postions that are not withdrawn yet
    Fees2 = Fees2[(Fees2['withdrawnToken0']!=0) & (Fees2['withdrawnToken1']!=0)]


    A = pd.merge(Fees2, Burns[['amount0', 'amount1', 'timestamp', 'origin', 'tickLower', 'tickUpper']], on = ['origin', 'tickLower', 'tickUpper'] , how = "inner")

    #sum burn amounts

    B = A[(A['withdrawnToken0']==-1*A['amount0']) & (A['withdrawnToken1']==-1*A['amount1'])]


    C = B[(B['FeesToken0']!=-999.0) & (B['FeesToken1']!=-999.0)]


    Pools2 = Pools.rename(columns={'timestamp': 'timestamp_y'})

    D = pd.merge(C, Pools2[['token1Price', 'token0Price', 'timestamp_y']], on = ['timestamp_y'] , how = "inner")


    R = pd.DataFrame()
    R['V0'] = D['depositedToken0'] + D['depositedToken1']*D['token0Price_x']
    R['VT'] = D['withdrawnToken0'] + D['withdrawnToken1']*D['token0Price_y']
    R['F'] = D['FeesToken0'] + D['FeesToken1']*D['token0Price_y']
    R['Tot'] = ( (R['VT'] + R['F']) / R['V0'] -1 ) #* 100

    R['InvHolRet'] = ( (D['depositedToken0'] + D['depositedToken1']*D['token0Price_y'])/R['V0']) - 1
    R['AdSelCos'] = (R['VT']/R['V0'] - 1) - R['InvHolRet']
    R['FeeYield'] = R['F']/R['V0']

    R['Tot2'] = R['InvHolRet'] + R['AdSelCos'] + R['FeeYield']

    return A, B, C, D, R