import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import time
import os
import numpy as np
from Mint import *

# #convert sqrtPriceX96 to prices
# # sqrtPriceX96: The current price of the pool as a sqrt(token1/token0) Q64.96 value
# # Might not be accurate as of now since errors = coerce is used
# A = pd.to_numeric(DATAFRAME1['sqrtPriceX96'], errors='coerce')
# DATAFRAME1['prices'] = pow((A / pow(2, 96)), 2) * pow(10, 6-18) # 6-18 = decimals0 - decimals1
# #prices expressed in currency of token1: 1 USDC (token0) = 0.00056 ETH (token1)
#
# #convert timestamp
# DATAFRAME1['timestamp'] = pd.to_datetime(DATAFRAME1['timestamp'],unit='s')


def CleanPool(Pools):
    Pools = Pools

    #Rename
    Pools = Pools.rename(columns={'date': 'timestamp'})
    Pools = Pools.rename(columns={'liquidity': 'liquidityInRange'})

    #Time rounding
    Pools['timestamp'] = pd.to_datetime(Pools['timestamp'], unit='s')
    Pools['timestamp'] = Pools['timestamp'].round('D')

    #Change Types
    Pools['volumeToken0'] = Pools['volumeToken0'].astype(float)
    Pools['volumeToken1'] = Pools['volumeToken1'].astype(float)
    Pools['liquidityInRange'] = Pools['liquidityInRange'].astype(float)

    #sqrt price
    A = pd.to_numeric(Pools['sqrtPrice'], errors='coerce')
    Pools['sqrtPrice'] = pow((A / pow(2, 96)), 2) * pow(10, 6 - 18)  # 6-18 = decimals0 - decimals1

    Pools = Pools.rename(columns={'sqrtPrice': 'Prices'})

    Pools['Prices'] = Pools['Prices'].astype(float)

    return Pools

def CleanMints(Mints):
    Mints['timestamp'] = pd.to_datetime(Mints['timestamp'], unit='s')
    Mints['timestamp'] = Mints['timestamp'].round('D')

    #Change types
    Mints['amount0'] = Mints['amount0'].astype(float)
    Mints['amount1'] = Mints['amount1'].astype(float)
    Mints['amount'] = Mints['amount'].astype(float)
    Mints['amount']  = Mints['amount'] * pow(10, 6 - 18)
    Mints['tickLower'] = Mints['tickLower'].astype(int)
    Mints['tickUpper'] = Mints['tickUpper'].astype(int)

    return Mints

def CleanBurns(Burns):
    Burns['timestamp'] = pd.to_datetime(Burns['timestamp'], unit='s')
    Burns['timestamp'] = Burns['timestamp'].round('D')

    #Change types
    Burns['amount0'] = Burns['amount0'].astype(float)
    Burns['amount1'] = Burns['amount1'].astype(float)
    Burns['amount'] = Burns['amount'].astype(float)
    Burns['amount']  = Burns['amount'] * pow(10, 6 - 18)
    Burns['tickLower'] = Burns['tickLower'].astype(int)
    Burns['tickUpper'] = Burns['tickUpper'].astype(int)

    # Change burns to negatives
    Burns['amount0'] = Burns['amount0'] * -1
    Burns['amount1'] = Burns['amount1'] * -1
    Burns['amount'] = Burns['amount'] * -1

    return Burns

def CleanSwaps(Swaps):
    Swaps['timestamp'] = pd.to_datetime(Swaps['timestamp'], unit='s')

    #convert sqrtPriceX96 to prices
    A = pd.to_numeric(Swaps['sqrtPriceX96'], errors='coerce')
    Swaps['sqrtPriceX96'] = pow((A / pow(2, 96)), 2) * pow(10, 6-18) # 6-18 = decimals0 - decimals1
    Swaps = Swaps.rename(columns={'sqrtPriceX96': 'Prices'})

    return Swaps

def CleanPoolV2(Pools):
    Pools = Pools

    #Rename
    Pools = Pools.rename(columns={'date': 'timestamp'})

    #Time rounding
    Pools['timestamp'] = pd.to_datetime(Pools['timestamp'], unit='s')
    Pools['timestamp'] = Pools['timestamp'].round('D')

    #Change Types
    Pools['reserve0'] = Pools['reserve0'].astype(float)
    Pools['reserve1'] = Pools['reserve1'].astype(float)
    Pools['liquidityInRange'] = np.sqrt(Pools['reserve0']*Pools['reserve1'])
    Pools['reserveUSD'] = Pools['reserveUSD'].astype(float)

    Pools['Prices'] = Pools['reserve1']/Pools['reserve0']

    return Pools

def CleanFees(Fees):
    Fees = Fees

    #Rename
    Fees = Fees.rename(columns={'transaction.timestamp': 'timestamp'})
    Fees = Fees.rename(columns={'tickLower.tickIdx': 'tickLower'})
    Fees = Fees.rename(columns={'tickUpper.tickIdx': 'tickUpper'})
    Fees = Fees.rename(columns={'owner': 'origin'})


    #Time rounding
    Fees['timestamp'] = pd.to_datetime(Fees['timestamp'], unit='s')
    Fees['timestamp'] = Fees['timestamp'].round('D')

    #Change Types
    Fees['depositedToken0'] = Fees['depositedToken0'].astype(float)
    Fees['depositedToken1'] = Fees['depositedToken1'].astype(float)
    Fees['withdrawnToken0'] = Fees['withdrawnToken0'].astype(float)
    Fees['withdrawnToken1'] = Fees['withdrawnToken1'].astype(float)
    Fees['collectedFeesToken0'] = Fees['collectedFeesToken0'].astype(float)
    Fees['collectedFeesToken1'] = Fees['collectedFeesToken1'].astype(float)
    Fees['tickLower'] = Fees['tickLower'].astype(int)
    Fees['tickUpper'] = Fees['tickUpper'].astype(int)

    return Fees