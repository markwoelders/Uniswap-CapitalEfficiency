import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import time
import os
from Mint import *
from LP import *
from DataCleaning import *
from MarketDepth import *
from PoolContracts import *
from datetime import datetime
import numpy as np
import time

url2 = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'

#Load Data
Swaps = swaps(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
Pools = pools2(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
Mints = mint(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
Burns = burn(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")

#Save Data
# Swaps.to_pickle("SwapsFull")
# Pools.to_pickle("PoolsFull500")
# Mints.to_pickle("MintsFull500")
# Burns.to_pickle("BurnsFull500")

#Read Data
#Swaps = pd.read_pickle("SwapsFull")
#Pools = pd.read_pickle("PoolsFull500")
#Mints = pd.read_pickle("MintsFull500")
#Burns = pd.read_pickle("BurnsFull500")

#Clean Data
Swaps = CleanSwaps(Swaps)
Pools = CleanPool(Pools)
Mints = CleanMints(Mints)
Burns = CleanBurns(Burns)










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



######## Market Depth

# Call functions from other file
dfrgns, MintBurn = MarketDepthV3Part1(Pools = Pools, Mints = Mints, Burns = Burns, tickspacing=10, UpperCut=240000, LowerCut=170000,  decimals0 = 6, decimals1 = 18)

#dfrgns.to_pickle("dfrgnsBIG500!")
dfrgns3000 = pd.read_pickle("dfrgnsBIG3000")
dfrgns500 = pd.read_pickle("dfrgnsBIG500!")


liqdist500, dfdepth500 = MarketDepthV3Part2(dfrgns500, Pools=Pools, tickspacing=10, depthpct=0.02,  decimals0 = 6, decimals1 = 18)
liqdist3000, dfdepth3000 = MarketDepthV3Part2(dfrgns3000, Pools=Pools, tickspacing=60, depthpct=0.02,  decimals0 = 6, decimals1 = 18)


plt.plot(dfdepth500['timestamp'],  dfdepth500['depth'][0:391] + dfdepth3000['depth'][0:391] )
plt.plot(dfdepth3000['timestamp'],  dfdepth3000['depth'] )


PoolsV2 = pools2V2(timestamp_counter = 1654066800, pool = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc" )

PoolsV2 = CleanPoolV2(PoolsV2)

dfdepthV2 = MarketDepthV2(PoolsV2, 1.02)
dfdepthV2['depth'] = MarketDepthV2(PoolsV2, 1.02).depth + MarketDepthV2(PoolsV2, 0.98).depth
dfdepthV2 = dfdepthV2[0:391]

plt.plot(dfdepth500['timestamp'], dfdepth500['depth'][0:391] + dfdepth3000['depth'][0:391] ,label = "V3 USDC-ETH")
plt.plot(dfdepthV2['timestamp'], dfdepthV2['depth'], label = "V2 USDC-ETH")
plt.xlabel('Timestamp')
plt.ylabel("Depth $")
plt.xlim(xmin=dfdepth500.iloc[50,0])
plt.ylim(ymax=250e6, ymin = 0)
plt.legend(title='V2/V3')
plt.title('Market Depth V2 vs V3 : USDC-ETH +/-2% Price Impact ')
plt.show()

plt.plot(PoolsV2['timestamp'], PoolsV2['Prices'])

np.max(dfdepthV2)
np.max(PoolsV2)


#### Verification (Note in paper they aggregated all usdc-eth pools!, does that make sense?)
CHECK = pd.read_csv(r'/Users/markwoelders/Library/Mobile Documents/com~apple~CloudDocs/Documents/MSc. Mathematical Finance/Dissertation/depth_daily.csv')

CHECK2 = CHECK.loc[(CHECK['unit_token0'] == 'USDC') & (CHECK['token1'] == 'WETH') & (CHECK['fee'] == 500) &  (CHECK['pct'] == 0.02)]
CHECK2 = CHECK2.sort_values('date')

CHECK3 = CHECK.loc[(CHECK['unit_token0'] == 'USDC') & (CHECK['token1'] == 'WETH') & (CHECK['fee'] == 500) &  (CHECK['pct'] == -0.02)]
CHECK3 = CHECK3.sort_values('date')

B = CHECK2['marketdepth'].reset_index() + CHECK3['marketdepth'].reset_index()

CHECK2['timestamp'] = pd.to_datetime(CHECK2['date'])
CHECK2['timestamp'] = CHECK2['timestamp'].round('D')

plt.plot(CHECK2['timestamp'], B)

B.rolling(window =7).mean().plot()



########### LP Returns

Pools = pools2(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
Burns = burn(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8")
Fees = Fees(id_counter = 99999, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8" )

Pools = CleanPool(Pools)
Burns = CleanBurns(Burns)
Fees = CleanFees(Fees)

R = LPreturns(Fees, Pools, Burns)
R['Tot'].describe()

plt.hist(R['Tot'], bins = 50)
plt.xlabel('% Returns')
plt.ylabel("Number closed LP positions")
plt.title('Liquidity Provider Returns')
plt.show()

#
plt.hist(R['InvHolRet'], bins = 50, label = 'Inventory Holding Returns')
plt.hist(R['AdSelCos'], bins = 50 , label = 'Adverse Selection Costs')
plt.hist(R['FeeYield'], bins = 50, label = 'Fee Yield')
plt.xlabel('% Returns')
plt.ylabel("Number")
plt.title('Liquidity Provider Return splits')
plt.legend(title='Return Split')
plt.show()

#
A,B,C,D,R = LPreturns(Fees, Pools, Burns)
#



#V2

LiqSnaps = LiqSnapsV2(timestamp_counter = 1654066800, pool = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc" )

len(np.unique(LiqSnaps['user.id']))