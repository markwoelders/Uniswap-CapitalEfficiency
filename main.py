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
from LPDuration import *
from PoolContracts import *
from Ratios import *
from datetime import datetime
import numpy as np
import time
from matplotlib.ticker import PercentFormatter

url2 = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'

#Load Data
Swaps_1 = swaps(timestamp_counter = 1654041601,end = 1652041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_2 = swaps(timestamp_counter = 1652041601,end = 1650041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_3 = swaps(timestamp_counter = 1648041601,end = 1646041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_4 = swaps(timestamp_counter = 1646041601,end = 1644041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_5 = swaps(timestamp_counter = 1644041601,end = 1642041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_6 = swaps(timestamp_counter = 1642041601,end = 1640041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_7 = swaps(timestamp_counter = 1640041601,end = 1638041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_8 = swaps(timestamp_counter = 1638041601,end = 1636041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_9 = swaps(timestamp_counter = 1636041601,end = 1634041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_10 = swaps(timestamp_counter = 1634041601,end = 1632041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_11 = swaps(timestamp_counter = 1632041601,end = 1630041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_12 = swaps(timestamp_counter = 1630041601,end = 1628041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_13 = swaps(timestamp_counter = 1628041601,end = 1626041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_14 = swaps(timestamp_counter = 1626041601,end = 1624041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_15 = swaps(timestamp_counter = 1624041601,end = 1622041601, pool = ETHUSDCV3_500)
time.sleep(200)
Swaps_16 = swaps(timestamp_counter = 1622041601,end = 1620041601, pool = ETHUSDCV3_500)

Swaps = pd.concat([Swaps_1, Swaps_2, Swaps_3, Swaps_4, Swaps_5, Swaps_6,
                   Swaps_7, Swaps_8, Swaps_9,Swaps_10,Swaps_11,Swaps_12,Swaps_13,Swaps_14,
                   Swaps_15, Swaps_16], axis = 0)

Pools = pools2(timestamp_counter = 1654041601, pool = ETHUSDCV3_500)
Mints = mint(timestamp_counter = 1654041601, pool = ETHUSDCV3_500)
Burns = burn(timestamp_counter = 1654041601, pool = ETHUSDCV3_500)

Swaps3000 = swaps(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Pools3000 = pools2(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Mints3000 = mint(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Burns3000 = burn(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)

#Save Data
#Swaps.to_pickle("SwapsFull500!")
#Pools.to_pickle("PoolsFull500!")
#Mints.to_pickle("MintsFull500!")
#Burns.to_pickle("BurnsFull500!")

#Read Data
Swaps = pd.read_pickle("SwapsFull500!")
Pools = pd.read_pickle("PoolsFull500!")
Mints = pd.read_pickle("MintsFull500!")
Burns = pd.read_pickle("BurnsFull500!")

Swaps3000 = pd.read_pickle("SwapsFull3000")
Pools3000 = pd.read_pickle("PoolsFull3000")
Mints3000 = pd.read_pickle("MintsFull3000")
Burns3000 = pd.read_pickle("BurnsFull3000")

#Clean Data
Swaps = CleanSwaps(Swaps)
Pools = CleanPool(Pools)
Mints = CleanMints(Mints, timeround=True)
Burns = CleanBurns(Burns, timeround=True, negativeAmount=True)

Swaps3000 = CleanSwaps(Swaps3000)
Pools3000 = CleanPool(Pools3000)
Mints3000 = CleanMints(Mints3000, timeround=True)
Burns3000 = CleanBurns(Burns3000, timeround=True, negativeAmount=True)



######## Duration Liquidity Positions
# Note data should contain nonrounded timestamp
Mints500 = CleanMints(pd.read_pickle("MintsFull500!"), timeround=False)
Burns500 = CleanBurns(pd.read_pickle("BurnsFull500!"), timeround=False, negativeAmount=False)
DurationV3_500 = durationv3(Mints, Burns)
Mints3000 = CleanMints(pd.read_pickle("MintsFull3000"), timeround=False)
Burns3000 = CleanBurns(pd.read_pickle("BurnsFull3000"), timeround=False, negativeAmount=False)
DurationV3_3000 = durationv3(Mints, Burns)

mintsV3 = pd.concat([Mints500, Mints3000], axis = 0)
burnsV3 = pd.concat([Burns500, Burns3000], axis = 0)

mintsV2 = mintV2(timestamp_counter = 1654041601, end=1620259200, pool = ETHUSDCV2 )
burnsV2 = burnV2(timestamp_counter = 1654041601, end=1620259200, pool = ETHUSDCV2 )
mintsV2 = mintsV2.rename(columns={'to': 'origin'})
burnsV2 = burnsV2.rename(columns={'sender': 'origin'})

DurationV2 = durationv2(mintsV2, burnsV2)
DurationV3 = durationv3(mintsV3, burnsV3)

DurationV2.describe()
DurationV3.describe()

DurationV3 = DurationV3[DurationV3.astype(int) > 0 ]

plt.hist(DurationV2.astype('timedelta64[D]'), weights=np.ones(len(DurationV2.astype('timedelta64[D]'))) / len(DurationV2.astype('timedelta64[D]')), bins = 60, label = 'V2', alpha = 0.9, color = "deeppink")
plt.hist(DurationV3.astype('timedelta64[D]'), weights=np.ones(len(DurationV3.astype('timedelta64[D]'))) / len(DurationV3.astype('timedelta64[D]')), bins = 30, label = 'V3', alpha = 0.6, color = "mediumorchid")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Duration (Days)')
plt.ylabel("% LP positions")
plt.title('Liquidity Position Duration')
plt.legend()
plt.show()



#Issue: We assume now that all liquidity is burned at the maximum date of the dataset, however, that actually has not happened!
#Therefore, we might underestimate the actual duration of liquidity positions.



######## Swap burn ratio's V3
swapsV2_1 = swapV2(timestamp_counter = 1654041601, end = 1634041601 ,  pool = '"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"' )
swapsV2_2 = swapV2(timestamp_counter = 1634041601, end = 1620259200 ,  pool = '"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"' )
swapsV2 = pd.concat([swapsV2_1, swapsV2_2], axis=0)


#V2
FrequenciesSwap_MintsV2, FrequenciesSwap_BurnsV2, FrequenciesMints_BurnsV2, Freq_RatioSwap_MintsV2, Freq_RatioSwap_BurnsV2, Freq_RatioMints_BurnsV2  = ratiosV2(swapsV2, mintsV2, burnsV2)
#V3
swapsV3 = pd.concat([Swaps, Swaps3000], axis = 0)
mintsV3 = pd.concat([Mints, Mints3000], axis = 0)
burnsV3 = pd.concat([Burns, Burns3000], axis = 0)
FrequenciesSwap_Mints, FrequenciesSwap_Burns, FrequenciesMints_Burns, Freq_RatioSwap_Mints, Freq_RatioSwap_Burns, Freq_RatioMints_Burns  = ratiosV3(swapsV3, mintsV3, burnsV3)

#Swap Mint ratio
plt.plot(FrequenciesSwap_Mints['timestamp'], Freq_RatioSwap_Mints, label = 'V3', color = 'mediumorchid')
plt.plot(FrequenciesSwap_MintsV2['timestamp'], Freq_RatioSwap_MintsV2, label = 'V2', color = 'deeppink')
plt.xlabel('Date')
plt.ylabel("Swap/Mint Ratio")
plt.title('Number of swaps per mint per day')
plt.legend()
plt.show()

#Big difference comes from Mints per day, not from swaps per day
#Swaps
FrequenciesSwap_Mints['Frequency_x'].describe()
FrequenciesSwap_MintsV2['Frequency_x'].describe()
#Mints
FrequenciesSwap_Mints['Frequency_y'].describe()
FrequenciesSwap_MintsV2['Frequency_y'].describe()

#Swap Burn ratio
plt.plot(FrequenciesSwap_Burns['timestamp'], Freq_RatioSwap_Burns, label = 'V3', color = 'mediumorchid')
plt.plot(FrequenciesSwap_BurnsV2['timestamp'], Freq_RatioSwap_BurnsV2, label = 'V2', color = 'deeppink')
plt.xlabel('Date')
plt.ylabel("Swap/Burn Ratio")
plt.title('Number of swaps per burn per day')
plt.legend()
plt.show()

FrequenciesSwap_Burns['Frequency_y'].describe()
FrequenciesSwap_BurnsV2['Frequency_y'].describe()

#Mint Burn ratio
plt.plot(FrequenciesMints_Burns['timestamp'], Freq_RatioMints_Burns, label = 'V3', color = 'mediumorchid')
plt.plot(FrequenciesMints_BurnsV2['timestamp'], Freq_RatioMints_BurnsV2, label = 'V2', color = 'deeppink')
plt.xlabel('Date')
plt.ylabel("Mint/Burn Ratio")
plt.title('Number of mints per burn per day')
plt.legend()
plt.show()


# Liquidity math (A == B according to core paper, hence L in paper is amount)
A = (Mints['amount0'] + (Mints['amount'] * pow(10, 6-18))/np.sqrt(1.0001 ** Mints['tickUpper'] * pow(10, 6-18))  ) * ( Mints['amount1'] + Mints['amount'] * pow(10, 6-18) * np.sqrt(1.0001 ** Mints['tickLower'] * pow(10, 6-18))   )
B = (Mints['amount'] * pow(10, 6-18)) ** 2


######## Market Depth convert to something daily

# Call functions from other file
dfrgns, MintBurn = MarketDepthV3Part1(Pools = Pools, Mints = Mints, Burns = Burns, tickspacing=10, UpperCut=240000, LowerCut=170000,  decimals0 = 6, decimals1 = 18)

#save data after MarketDepthV3Part1
#dfrgns.to_pickle("dfrgnsBIG500!")
dfrgns3000 = pd.read_pickle("dfrgnsBIG3000")
dfrgns500 = pd.read_pickle("dfrgnsBIG500!")

#MarketDepthV3Part2
liqdist500, dfdepth500 = MarketDepthV3Part2(dfrgns500, Pools=Pools, tickspacing=10, depthpct=0.02,  decimals0 = 6, decimals1 = 18)
liqdist3000, dfdepth3000 = MarketDepthV3Part2(dfrgns3000, Pools=Pools3000, tickspacing=60, depthpct=0.02,  decimals0 = 6, decimals1 = 18)


#MarketDepth V2
PoolsV2 = pools2V2(timestamp_counter = 1654041601, pool = ETHUSDCV2 )
PoolsV2 = CleanPoolV2(PoolsV2)

#+/- 2% price impact
dfdepthV2 = MarketDepthV2(PoolsV2, 1.02)
dfdepthV2['depth'] = MarketDepthV2(PoolsV2, 1.02).depth + MarketDepthV2(PoolsV2, 0.98).depth
dfdepthV2 = dfdepthV2[0:391]
dfdepthV2 = dfdepthV2.iloc[::-1] #Reverse dataframe to start with lowest timestamp


# X_axis = np.arange(len(DepthDistribution.loc[:,2]))
# width = 0.35
# fig, ax = plt.subplots()
# ax.bar(X_axis - width/2, DepthDistribution.loc[:,0], width, label = "V3 (0.05% + 0.3% pool)", color = 'mediumorchid')
# ax.tick_params(axis='y', labelcolor='mediumorchid')
# ax2 = ax.twinx()
# ax2.bar(X_axis + width/2, DepthDistribution.loc[:,1], width, label = "V2", color = 'deeppink')
# ax2.tick_params(axis='y', labelcolor='deeppink')
# plt.ylim(ymin=0)
# fig.legend(title='V2/V3')
# plt.xticks(X_axis, DepthDistribution.loc[:,2])
# plt.title('Mean Market Depth per Price Impact')
# plt.xlabel("+/- % from spot")
# plt.ylabel("Depth ($mm)")
# plt.show()


fig, ax = plt.subplots()
ax.plot(dfdepth500['timestamp'], ((dfdepth500['depth'][0:391] + dfdepth3000['depth'][1:391]))/1000000 ,label = "V3 (0.05% + 0.3% pool)", color = 'mediumorchid')
ax.tick_params(axis='y', labelcolor='mediumorchid')
ax2 = ax.twinx()
ax2.plot(dfdepth500['timestamp'], dfdepthV2['depth']/1000000, label = "V2", color = 'deeppink')
ax2.tick_params(axis='y', labelcolor='deeppink')
plt.ylim(ymin=0)
fig.legend(title='V2/V3')
plt.title('Market Depth V2 vs V3 : +/-2% Price Impact ')
plt.xlabel("Date")
plt.ylabel("Depth ($mm)")
plt.show()

#Descriptive Stats
(((dfdepth500['depth'][0:391] + dfdepth3000['depth'][1:391]))/1000000).describe() # In millions of USD
(dfdepthV2['depth']/1000000).describe() # In millions of USD


#### Volatility
# calculate daily logarithmic return
Pools = Pools[0:len(Pools)-1]
returns = (np.log(Pools.Prices / Pools.Prices.shift(-1)))

# calculate daily standard deviation of returns
daily_std = returns.rolling(24).std()

plt.plot(Pools['timestamp'], daily_std)

# annualized daily standard deviation
std = daily_std * 252 ** 0.5

daily_std.describe()[6]
daily_std_dummy = np.where(daily_std >= daily_std.describe()[6], 1, 0)



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



########### LP Returns V3

#Load data
Pools3000 = pools2(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Burns3000 = burn(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Fees3000 = fees(id_counter = 99999, pool = ETHUSDCV3_3000 )

Pools500 = pools2(timestamp_counter = 1654041601, pool = ETHUSDCV3_500)
Burns500 = burn(timestamp_counter = 1654041601, pool = ETHUSDCV3_500)
Fees500 = fees(id_counter = 99999, pool = ETHUSDCV3_500 )

#Clean data
Pools3000 = CleanPool(Pools3000)
Burns3000 = CleanBurns(Burns3000, timeround = True, negativeAmount = True)
Fees3000 = CleanFees(Fees3000)

Pools500 = CleanPool(Pools500)
Burns500 = CleanBurns(Burns500, timeround = True, negativeAmount = True)
Fees500 = CleanFees(Fees500)

#Calculate Returns
Rv3_3000 = LPreturns(Fees3000, Pools3000, Burns3000)
Rv3_500 = LPreturns(Fees500, Pools500, Burns500)
Rv3 = pd.concat([Rv3_3000, Rv3_500], axis=0)

#Delete potential wrong calculations
Rv3 = Rv3[Rv3['VT']>0]
Rv3 = Rv3[Rv3['AdSelCos']<0.001]

DesV3 = Rv3.describe()
Rv3['FeeYield'].describe()
Rv3['AdSelCos'].describe()
Rv3['InvHolRet'].describe()


#Plot Returns from just fees
plt.hist(Rv3['FeeYield'], bins = 50, color = "pink")
plt.xlabel('Returns')
plt.ylabel("Number closed LP positions")
plt.title('Liquidity Provider Returns V3')
plt.show()

#Plot Return decomposition
plt.hist(Rv3['InvHolRet'], weights=np.ones(len(Rv3['InvHolRet'])) / len(Rv3['InvHolRet']), bins = 60, label = 'Inventory Holding Returns', alpha = 1, color = "deepskyblue")
plt.hist(Rv3['AdSelCos'], weights=np.ones(len(Rv3['AdSelCos'])) / len(Rv3['AdSelCos']), bins = 75 , label = 'Adverse Selection Costs', alpha=0.7, color = "red")
plt.hist(Rv3['FeeYield'], weights=np.ones(len(Rv3['FeeYield'])) / len(Rv3['FeeYield']), bins = 20, label = 'Fee Yield', alpha = 0.7, color = "lawngreen")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=-0.5, xmax = 0.5)
plt.ylabel("% closed LP positions")
plt.title('Liquidity Provider Return Split V3')
plt.legend(title='Return Split V3')
plt.show()


#LP Returns V2
LiqSnaps = LiqSnapsV2(timestamp_counter = 1654041601, pool = ETHUSDCV2 )
mintsV2 = mintV2(timestamp_counter = 1654041601, pool = ETHUSDCV2 )
burnsV2 = burnV2(timestamp_counter = 1654041601, pool = ETHUSDCV2)
Rv2 = LPreturnsV2(LiqSnaps, burnsV2, mintsV2)

Rv2 = Rv2[Rv2['AdSelCos']<0.001]
Rv2['F'].describe()
DesV2 = Rv2.describe()

#Plot Returns from just fees
plt.hist(Rv2['F'], weights=np.ones(len(Rv2['F'])) / len(Rv2['F']), bins = 30, label = 'v2', color = "deeppink", alpha =0.9)
plt.hist(Rv3['FeeYield'], weights=np.ones(len(Rv3['FeeYield'])) / len(Rv3['FeeYield']), bins = 54, label = 'v3', alpha = 0.8, color = "mediumorchid")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=0, xmax = 0.3)
plt.ylabel("% closed LP positions")
plt.title('Liquidity Provider Fee Returns')
plt.legend(title='V2/V3')
plt.show()


#Plot Return decomposition
plt.hist(Rv2['InvHolRet'], weights=np.ones(len(Rv2['InvHolRet'])) / len(Rv2['InvHolRet']), bins = 47, label = 'Inventory Holding Returns', alpha = 1, color = "deepskyblue")
plt.hist(Rv2['AdSelCos'], weights=np.ones(len(Rv2['AdSelCos'])) / len(Rv2['AdSelCos']), bins = 6 , label = 'Adverse Selection Costs', alpha=0.7, color = "red")
plt.hist(Rv2['F'], weights=np.ones(len(Rv2['F'])) / len(Rv2['F']), bins = 10, label = 'Fee Yield', alpha = 0.7, color = "lawngreen")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=-0.5, xmax = 0.5)
plt.ylabel("% closed LP positions")
plt.title('Liquidity Provider Return Split V2')
plt.legend(title='Return Split V2')
plt.show()
