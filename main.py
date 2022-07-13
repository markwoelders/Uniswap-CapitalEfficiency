import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import time
import os
from Mint import *
from DataCleaning import *
from MarketDepth import *
from PoolContracts import *
from datetime import datetime
import numpy as np
import time

url2 = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'

#Load Data
Swaps = swaps(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
Pools = pools2(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
Mints = mint(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
Burns = burn(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")

#Save Data
#Swaps.to_pickle("SwapsFull")
#Pools.to_pickle("PoolsFull")
#Mints.to_pickle("MintsFull")
#Burns.to_pickle("BurnsFull")

#Read Data
#Swaps = pd.read_pickle("SwapsFull")
#Pools = pd.read_pickle("PoolsFull")
#Mints = pd.read_pickle("MintsFull")
#Burns = pd.read_pickle("BurnsFull")

#Clean Data
Pools = CleanPool(Pools)
Swaps = CleanSwaps(Swaps)
Mints = CleanMints(Mints)
Burns = CleanBurns(Burns)



#########
Mints['tickDiff'] = ( (Mints['tickUpper'] - Mints['tickLower'])/10).astype(int)

Mints['Amount0Tick'] =  Mints['amount0'] / Mints['tickDiff']
Mints['Amount1Tick'] = Mints['amount1'] / Mints['tickDiff']

Ticks = pd.DataFrame()
Ticks['TICKS'] = Mints.apply(lambda x: (np.arange(x['tickLower'],  x['tickUpper'], 10)), axis=1)

Mints = pd.concat([Mints, Ticks], axis=1)

# For simplicity remove rows with big tickDif (aka all inf liquiddity prov) and/or where tickLower is below 0
# This is done to deal with memory issues
Mints = Mints[Mints.tickDiff < 5000]
Mints = Mints[Mints.tickLower > 0]

E = np.arange(min(Mints['tickLower']),  max(Mints['tickUpper']), 10)

Trial = Mints.apply(lambda x:  pd.DataFrame( (x['Amount0Tick'] * np.ones(len(x['TICKS'])) ).reshape(1,len(x['TICKS'])), columns = x['TICKS']), axis=1)

L = [x for x in Trial]

#try for looping in chunks and concat
LL = L[1:1000]

Result = pd.concat(L, axis = 0, ignore_index=True).reindex(columns=E.tolist()).fillna(0)
Result = Result.join(Mints['timestamp'], how='inner')

Result['timestamp'] = Result['timestamp'].round('D')

DF = Result.groupby('timestamp').sum()
DF2 = DF.cumsum()






######## Duration Liquidity Position
# Note data should contain nonrounded timestamp

A = pd.merge(Mints, Burns, on=["origin", "tickLower", "tickUpper", "amount"], how = "left")

#Fill na with max date since these mint transactions have no corresponding burns yet, aka liquidity still avtive
A["timestamp_y"]= A["timestamp_y"].fillna(np.max(Burns["timestamp"]))

B = A["timestamp_y"] - A["timestamp_x"]

plt.hist(B.astype('timedelta64[h]'), bins = 100, cumulative=False)
B.describe()

#Issue: We assume now that all liquidity is burned at the maximum date of the dataset, however, that actually has not happened!
#Therefore, we might underestimate the actual duration of liquidity positions.

######## Swap burn ratio's

Swaps['timestamp'] = Swaps['timestamp'].round('D')
Mints['timestamp'] = Mints['timestamp'].round('D')
Burns['timestamp'] = Burns['timestamp'].round('D')

SwapsFreq = pd.DataFrame()
MintsFreq = pd.DataFrame()
BurnsFreq = pd.DataFrame()

SwapsFreq['Frequency'] = Swaps.groupby(['timestamp']).size()
MintsFreq['Frequency'] = Mints.groupby(['timestamp']).size()
BurnsFreq['Frequency'] = Burns.groupby(['timestamp']).size()

SwapsFreq['timestamp'] = SwapsFreq.index
MintsFreq['timestamp'] = MintsFreq.index
BurnsFreq['timestamp'] = BurnsFreq.index

SwapsFreq = SwapsFreq.reset_index(drop=True)
MintsFreq = MintsFreq.reset_index(drop=True)
BurnsFreq = BurnsFreq.reset_index(drop=True)

FrequenciesSwap_Mints = pd.merge(SwapsFreq, MintsFreq, on = ['timestamp'] , how = "inner")
FrequenciesSwap_Burns = pd.merge(SwapsFreq, BurnsFreq, on = ['timestamp'] , how = "inner")
FrequenciesMints_Burns = pd.merge(MintsFreq, BurnsFreq, on = ['timestamp'] , how = "inner")


#Number of swaps per number of mints per day
Freq_RatioSwap_Mints = FrequenciesSwap_Mints['Frequency_x'] / FrequenciesSwap_Mints['Frequency_y']
#Number of swaps per number of burns per day
Freq_RatioSwap_Burns = FrequenciesSwap_Burns['Frequency_x'] / FrequenciesSwap_Burns['Frequency_y']
#Number of mints per number of burns per day
Freq_RatioMints_Burns = FrequenciesMints_Burns['Frequency_x'] / FrequenciesMints_Burns['Frequency_y']


plt.plot(FrequenciesSwap_Mints['timestamp'], Freq_RatioSwap_Mints)
plt.plot(FrequenciesSwap_Burns['timestamp'], Freq_RatioSwap_Burns)
plt.plot(FrequenciesMints_Burns['timestamp'], Freq_RatioMints_Burns)

#Swaps vs Mints
plt.plot(FrequenciesSwap_Mints['timestamp'], FrequenciesSwap_Mints['Frequency_x'])
plt.plot(FrequenciesSwap_Mints['timestamp'], FrequenciesSwap_Mints['Frequency_y'])

#Mints vs Burns
plt.plot(FrequenciesMints_Burns['timestamp'], FrequenciesMints_Burns['Frequency_x'])
plt.plot(FrequenciesMints_Burns['timestamp'], FrequenciesMints_Burns['Frequency_y'])


# Liquidity math (A == B according to core paper, hence L in paper is amount)
A = (Mints['amount0'] + (Mints['amount'] * pow(10, 6-18))/np.sqrt(1.0001 ** Mints['tickUpper'] * pow(10, 6-18))  ) * ( Mints['amount1'] + Mints['amount'] * pow(10, 6-18) * np.sqrt(1.0001 ** Mints['tickLower'] * pow(10, 6-18))   )
B = (Mints['amount'] * pow(10, 6-18)) ** 2





####
MintBurn = pd.concat([Mints, Burns], axis = 0)
MintBurn.describe()
MintBurn = MintBurn[MintBurn.tickLower > 170000]
MintBurn = MintBurn[MintBurn.tickLower < 240000]

def genLiqRange(dft, tickspacing=60):
    #' Generate Liquidity Distribution (liquidity amount) based on default tick size
    #' returns dataframe with columns 'tickLower', 'amount', 'price'
    if not (
        all(dft.tickLower % tickspacing == 0) & all(dft.tickUpper % tickspacing == 0)
    ):
        print("fail tick window")
    rgn = np.arange(dft.tickLower.min(), dft.tickUpper.max() + 1, tickspacing)
    dfrgn = pd.DataFrame({"tickLower": rgn, "amount": 0}).set_index("tickLower")
    for i, row in dft.iterrows():
        dfrgn.loc[np.arange(row.tickLower, row.tickUpper, tickspacing), "amount"] = (
            dfrgn.loc[np.arange(row.tickLower, row.tickUpper, tickspacing), "amount"]
            + row["amount"]
        )  # /(row['tickUpper']-row['tickLower'])
    dfrgn = dfrgn.reset_index()
    dfrgn["price"] = 1e12 / (1.0001 ** dfrgn.tickLower)
    return dfrgn

def genLiqRangeOverTime(df, tickspacing=60):
    #' Generate multiple dates of liquidity range
    dfs = df.groupby(["timestamp", "tickLower", "tickUpper"]).amount.sum()
    dft2 = pd.DataFrame(dfs).reset_index()
    rgns = dict()
    for dt in dft2["timestamp"].unique():
        rgns[dt] = genLiqRange(dft2.loc[dft2.timestamp <= dt], tickspacing=tickspacing)
    print('done, start concat')
    dfrgns = pd.concat(rgns).droplevel(1).rename_axis("timestamp")
    return dfrgns

start = time.time()
dfrgns = genLiqRangeOverTime(MintBurn)
end = time.time()

print("algo took", end-start)

#dfrgns.to_pickle("dfrgnsBIG")
dfrgns = pd.read_pickle("dfrgnsBIG")

dfrgns['amount'] = dfrgns['amount'] / pow(10, 6 - 18)

def genLiqRangeXNumeraire(dfrgn, tickspacing=60, alt=1):
    #' convert liquidity amount to dollar liquidity amount
    #' input dfrgn is a dataframe of liquidity distribution with columns 'tickLower', 'amount', 'P' as current price,  ['price' associated with tick]
    #' returns dataframe with columns: 'tickLower', 'amount', 'price', 'P', 'pa', 'pb', 'p', 'x', 'y',
    #' 'liqX', 'depth'
    dfrgn = dfrgn.assign(pa=lambda x: 1.0001 ** x.tickLower)
    dfrgn = dfrgn.assign(pb=lambda x: 1.0001 ** (x.tickLower + tickspacing))
    # dfrgn=dfrgn.assign(x=lambda x: x.amount*(x.pb**0.5-x.pa**0.5)/((x.pa*x.pb)**0.5))
    dfrgn["p"] = 1 / dfrgn.P * 1e12

    dfrgn.loc[dfrgn.p <= dfrgn.pa, "x"] = ( dfrgn.amount * (dfrgn.pb ** 0.5 - dfrgn.pa ** 0.5) / ((dfrgn.pa * dfrgn.pb) ** 0.5) / 1e6 )
    dfrgn.loc[(dfrgn.pa < dfrgn.p) & (dfrgn.p < dfrgn.pb), "x"] = ( dfrgn.amount * (dfrgn.pb ** 0.5 - dfrgn.p ** 0.5) / ((dfrgn.p * dfrgn.pb) ** 0.5) / 1e6 )
    dfrgn.loc[dfrgn.p >= dfrgn.pb, "x"] = 0

    dfrgn.loc[dfrgn.p <= dfrgn.pa, "y"] = 0
    dfrgn.loc[(dfrgn.pa < dfrgn.p) & (dfrgn.p < dfrgn.pb), "y"] = ( dfrgn.amount * (dfrgn.p ** 0.5 - dfrgn.pa ** 0.5) / 1e18 )
    dfrgn.loc[dfrgn.p >= dfrgn.pb, "y"] = ( dfrgn.amount * (dfrgn.pb ** 0.5 - dfrgn.pa ** 0.5) / 1e18 )

    if alt:
        # note that here P is the current price rather than the price at each tick
        dfrgn["liqX"] = 1 * dfrgn.x + dfrgn.P * dfrgn.y
    else:
        # convert liquidity using price at lower tick
        dfrgn["liqX"] = 1 * dfrgn.x + dfrgn.price * dfrgn.y
    return dfrgn


dfprice = Pools[['timestamp', 'token0Price']].rename(columns={"token0Price": "P"})
dfprice['P'] = dfprice['P'].astype(float)

dfrgns = dfrgns.reset_index()
dfrgns = pd.merge(dfrgns, dfprice, on="timestamp")

def genDepth(dfrgn, tickspacing=60,alt=1):
    #' Calculate dollar liquidity amount distribution and depth chart
    dfrgnD = genLiqRangeXNumeraire(dfrgn, tickspacing=tickspacing,alt=alt)
    #' Generate depth of any liquidity range/distribution by integrating across the range
    df1 = dfrgnD.loc[dfrgnD.price > dfrgnD.P].sort_values("price")
    df2 = dfrgnD.loc[dfrgnD.price <= dfrgnD.P].sort_values("price")
    df2["depth"] = df2.loc[::-1, "liqX"].cumsum()[::-1]
    df1 = df1.assign(depth=lambda x: x.liqX.cumsum())
    dfm = pd.concat([df2, df1])

    # depth=dfm.loc[(dfm.price>=dfm.P*0.97) & (dfm.price<=dfm.P*1.03)].liqX.sum()
    return dfm#[["date", "tickLower", "price", "P", "liqX", "depth"]]


def genDepthOverTime(dfrgns, tickspacing=60):
    #' Calls genDepth to getnerate full range of liquidity distribution, dollar liquidity and depth
    liqdist = list()
    for dt in dfrgns["timestamp"].unique():
        liqdist.append(
            genDepth(dfrgns.loc[dfrgns.timestamp == dt], tickspacing=tickspacing)
        )
    #     pd.concat(liqdist).to_pickle('data/liqdist_eth.pkl')
    dfliqdist = pd.concat(liqdist)
    return dfliqdist


liqdist = pd.DataFrame(genDepthOverTime(dfrgns, tickspacing=60))

#make possibility to split +2% and -2%
def fillGranularDistribution(df, depthpct=0.02, returnDepth=1, tickspacing=60):
    # expand liquidity distribution granularity to 1 tick, effectively resampling tickspacing to 1 tick
    dft = df.copy()
    P = dft.P.values[0]
    if depthpct == 1:
        P_u = 0
        P_l = 1e9
        tick_u = dft.tickLower.max()
        tick_l = dft.tickLower.min()
    else:
        P_u = dft.P.values[0] * (1 - depthpct)
        P_l = dft.P.values[0] * (1 + depthpct)
        tick_u = (
            int(np.log((1 / P_u * 1e12)) / np.log(1.0001) / tickspacing) + 1
        ) * tickspacing
        tick_l = (
            int(np.log((1 / P_l * 1e12)) / np.log(1.0001) / tickspacing) - 1
        ) * tickspacing

    dft["liqXpertick"] = dft["liqX"] / tickspacing
    dft["tick_u"] = tick_u
    dft["tick_l"] = tick_l

    rgn = np.arange(tick_l, tick_u + 1, 1)
    dfrgn = pd.DataFrame({"tickLower": rgn, "depthattick": 0})
    dfrgn["price"] = 1e12 / (1.0001 ** dfrgn.tickLower)
    for i, row in dft[ ( (dft.tickLower >= dft.tick_l - tickspacing) & (dft.tickLower <= dft.tick_u + tickspacing) ) ].iterrows():
        dfrgn.loc[ (dfrgn.tickLower >= row.tickLower) & (dfrgn.tickLower <= row.tickLower + tickspacing), "depthattick", ] = row["liqXpertick"]
    depth = dfrgn[(dfrgn.price >= P_u) & (dfrgn.price <= P_l)].depthattick.sum()
    depthbid = dfrgn[(dfrgn.price >= P_u) & (dfrgn.price <= P)].depthattick.sum()
    depthask = dfrgn[(dfrgn.price >= P) & (dfrgn.price <= P_l)].depthattick.sum()
    if returnDepth:
        return depth, depthbid, depthask #, dfrgn.price, P_u, P_l, P, tick_u, tick_l, rgn
    else:
        return dfrgn[(dfrgn.price >= P_u) & (dfrgn.price <= P_l)]


def fillGranularDistributionOverTime(liqdist, depthpct=0.02, tickspacing=60):
    # Generate depth time series by calling fillGranularDistribution for all dates
    depths = list()
    for dt in liqdist.timestamp.unique():
        depths.append(
            fillGranularDistribution(
                liqdist.loc[liqdist.timestamp == dt],
                depthpct=depthpct,
                returnDepth=1,
                tickspacing=tickspacing,
            )
        )
    dfdepth = pd.DataFrame(depths, index=pd.to_datetime(liqdist.timestamp.unique())).rename(
        columns={0: "depth"}
    )
    dfdepth = dfdepth.reset_index().rename(
        columns={"index": "timestamp", 1: "depthbid", 2: "depthask"}
    )
    dfdepth["timestamp"] = pd.to_datetime(dfdepth.timestamp)
    return dfdepth


dfdepth = fillGranularDistributionOverTime(liqdist, depthpct=0.02, tickspacing=60)

plt.plot(dfdepth['timestamp'], dfdepth['depth'])

dfdepth['depth'].describe()

B.rolling(window =7).mean().plot()




# Call functions from other file
dfrgns, MintBurn = MarketDepthV3Part1(Pools = Pools, Mints = Mints, Burns = Burns, tickspacing=10, UpperCut=240000, LowerCut=170000,  decimals0 = 6, decimals1 = 18)

#dfrgns.to_pickle("dfrgnsBIG500")
#dfrgns = pd.read_pickle("dfrgnsBIG")


liqdist, dfdepth = MarketDepthV3Part2(dfrgns, Pools=Pools, tickspacing=10, depthpct=0.02,  decimals0 = 6, decimals1 = 18)


plt.plot(dfdepth['timestamp'], dfdepth['depth'])






#### Verification (Note in paper they probably aggregated all usdc-eth pools!, does that make sense?)
CHECK = pd.read_csv(r'/Users/markwoelders/Library/Mobile Documents/com~apple~CloudDocs/Documents/MSc. Mathematical Finance/Dissertation/depth_daily.csv')

CHECK['unit_token0'] = 'USDC'
CHECK['unit_token1'] = 'WETH'

CHECK2 = CHECK.loc[(CHECK['unit_token0'] == 'USDC') & (CHECK['token1'] == 'WETH') & (CHECK['fee'] == 3000) &  (CHECK['pct'] == 0.02)]
CHECK2 = CHECK2.sort_values('date')

CHECK3 = CHECK.loc[(CHECK['unit_token0'] == 'USDC') & (CHECK['token1'] == 'WETH') & (CHECK['fee'] == 3000) &  (CHECK['pct'] == -0.02)]
CHECK3 = CHECK3.sort_values('date')

B = CHECK2['marketdepth'].reset_index() + CHECK3['marketdepth'].reset_index()

CHECK2['timestamp'] = pd.to_datetime(CHECK2['date'])
CHECK2['timestamp'] = CHECK2['timestamp'].round('D')

plt.plot(CHECK2['timestamp'].round('D'), B)

B.rolling(window =7).mean().plot()