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
import researchpy as rp
import scipy.stats as stats

url2 = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
url = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'

### Load Data ####
# Swaps_1 = swaps(timestamp_counter = 1654041601,end = 1652041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_2 = swaps(timestamp_counter = 1652041601,end = 1650041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_3 = swaps(timestamp_counter = 1648041601,end = 1646041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_4 = swaps(timestamp_counter = 1646041601,end = 1644041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_5 = swaps(timestamp_counter = 1644041601,end = 1642041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_6 = swaps(timestamp_counter = 1642041601,end = 1640041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_7 = swaps(timestamp_counter = 1640041601,end = 1638041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_8 = swaps(timestamp_counter = 1638041601,end = 1636041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_9 = swaps(timestamp_counter = 1636041601,end = 1634041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_10 = swaps(timestamp_counter = 1634041601,end = 1632041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_11 = swaps(timestamp_counter = 1632041601,end = 1630041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_12 = swaps(timestamp_counter = 1630041601,end = 1628041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_13 = swaps(timestamp_counter = 1628041601,end = 1626041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_14 = swaps(timestamp_counter = 1626041601,end = 1624041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_15 = swaps(timestamp_counter = 1624041601,end = 1622041601, pool = ETHUSDCV3_500)
# time.sleep(200)
# Swaps_16 = swaps(timestamp_counter = 1622041601,end = 1620041601, pool = ETHUSDCV3_500)
#
# Swaps = pd.concat([Swaps_1, Swaps_2, Swaps_3, Swaps_4, Swaps_5, Swaps_6,
#                    Swaps_7, Swaps_8, Swaps_9,Swaps_10,Swaps_11,Swaps_12,Swaps_13,Swaps_14,
#                    Swaps_15, Swaps_16], axis = 0)

### Load Data ####
# 0.05%
Pools = pools2(timestamp_counter=1654041601, pool=ETHUSDCV3_500)
Mints = mint(timestamp_counter=1654041601, pool=ETHUSDCV3_500)
Burns = burn(timestamp_counter=1654041601, pool=ETHUSDCV3_500)
# 0.3%
Swaps3000 = swaps(timestamp_counter=1654041601, pool=ETHUSDCV3_3000)
Pools3000 = pools2(timestamp_counter=1654041601, pool=ETHUSDCV3_3000)
Mints3000 = mint(timestamp_counter=1654041601, pool=ETHUSDCV3_3000)
Burns3000 = burn(timestamp_counter=1654041601, pool=ETHUSDCV3_3000)

### Save Data for easy use ####
#
# Swaps.to_pickle("SwapsFull500!")
# Pools.to_pickle("PoolsFull500!")
# Mints.to_pickle("MintsFull500!")
# Burns.to_pickle("BurnsFull500!")
#
# Swaps3000.to_pickle("SwapsFull3000!")
# Pools3000.to_pickle("PoolsFull3000!")
# Mints3000.to_pickle("MintsFull3000!")
# Burns3000.to_pickle("BurnsFull3000!")

### Read Data ####
# 0.05%
Swaps = pd.read_pickle("SwapsFull500!")
Pools = pd.read_pickle("PoolsFull500!")
Mints = pd.read_pickle("MintsFull500!")
Burns = pd.read_pickle("BurnsFull500!")
# 0.3%
Swaps3000 = pd.read_pickle("SwapsFull3000")
Pools3000 = pd.read_pickle("PoolsFull3000")
Mints3000 = pd.read_pickle("MintsFull3000")
Burns3000 = pd.read_pickle("BurnsFull3000")

### Clean Data ####
Swaps = CleanSwaps(Swaps)
Pools = CleanPool(Pools)
Mints = CleanMints(Mints, timeround=True)
Burns = CleanBurns(Burns, timeround=True, negativeAmount=True)

Swaps3000 = CleanSwaps(Swaps3000)
Pools3000 = CleanPool(Pools3000)
Mints3000 = CleanMints(Mints3000, timeround=True)
Burns3000 = CleanBurns(Burns3000, timeround=True, negativeAmount=True)






######## Duration Liquidity Positions ###########
# Note data should contain nonrounded timestamp
# V3 Data Preparation
Mints500 = CleanMints(pd.read_pickle("MintsFull500!"), timeround=False)
Burns500 = CleanBurns(pd.read_pickle("BurnsFull500!"), timeround=False, negativeAmount=False)
Mints3000 = CleanMints(pd.read_pickle("MintsFull3000"), timeround=False)
Burns3000 = CleanBurns(pd.read_pickle("BurnsFull3000"), timeround=False, negativeAmount=False)

mintsV3 = pd.concat([Mints500, Mints3000], axis=0)
burnsV3 = pd.concat([Burns500, Burns3000], axis=0)

# V2 Data Preparation
mintsV2 = mintV2(timestamp_counter=1654041601, end=1620259200, pool=ETHUSDCV2)
burnsV2 = burnV2(timestamp_counter=1654041601, end=1620259200, pool=ETHUSDCV2)
mintsV2 = mintsV2.rename(columns={'to': 'origin'})
burnsV2 = burnsV2.rename(columns={'sender': 'origin'})

# Calculate Durations
DurationV2, originV2 = durationv2(mintsV2, burnsV2)
DurationV3, originV3 = durationv3(mintsV3, burnsV3)

DurationV3 = DurationV3[DurationV3.astype(int) > 0]  # Filter out wrong data

DurationV2.describe()
DurationV3.describe()

# t testing mean duration
summary, results = rp.ttest(group1=DurationV3.astype('timedelta64[D]'), group1_name="V3 Duration",
                            group2=DurationV2.astype('timedelta64[D]'), group2_name="V2 Duration")
print(results)

# Plot Duration Distribution
plt.hist(DurationV3.astype('timedelta64[D]'),
         weights=np.ones(len(DurationV3.astype('timedelta64[D]'))) / len(DurationV3.astype('timedelta64[D]')), bins=60,
         label='V3', alpha=0.9, color="mediumorchid")
plt.hist(DurationV2.astype('timedelta64[D]'),
         weights=np.ones(len(DurationV2.astype('timedelta64[D]'))) / len(DurationV2.astype('timedelta64[D]')), bins=60,
         label='V2', alpha=0.6, color="gray")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Duration (Days)')
plt.ylabel("% LP positions")
plt.title('Liquidity Position Duration')
plt.legend()
plt.show()

# Issue: We assume now that all liquidity is burned at the maximum date of the dataset, however, that actually has not happened!
# Therefore, we might underestimate the actual duration of liquidity positions.





######## Ratios #########
# V2 Data Preparation
swapsV2_1 = swapV2(timestamp_counter=1654041601, end=1634041601, pool='"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"')
swapsV2_2 = swapV2(timestamp_counter=1634041601, end=1620259200, pool='"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"')
swapsV2 = pd.concat([swapsV2_1, swapsV2_2], axis=0)

mintsV2 = mintV2(timestamp_counter=1654041601, end=1620259200, pool=ETHUSDCV2)
burnsV2 = burnV2(timestamp_counter=1654041601, end=1620259200, pool=ETHUSDCV2)
mintsV2 = mintsV2.rename(columns={'to': 'origin'})
burnsV2 = burnsV2.rename(columns={'sender': 'origin'})

# Calculate ratios and frequencies V2
FrequenciesSwap_MintsV2, FrequenciesSwap_BurnsV2, FrequenciesMints_BurnsV2, Freq_RatioSwap_MintsV2, Freq_RatioSwap_BurnsV2, Freq_RatioMints_BurnsV2 = ratiosV2(
    swapsV2, mintsV2, burnsV2)

# V3 Data Preparation
swapsV3 = pd.concat([Swaps, Swaps3000], axis=0)
mintsV3 = pd.concat([Mints, Mints3000], axis=0)
burnsV3 = pd.concat([Burns, Burns3000], axis=0)

# Calculate ratios and frequencies V2

FrequenciesSwap_Mints, FrequenciesSwap_Burns, FrequenciesMints_Burns, Freq_RatioSwap_Mints, Freq_RatioSwap_Burns, Freq_RatioMints_Burns = ratiosV3(
    swapsV3, mintsV3, burnsV3)

# Unique LD's and LP's
len(np.unique(mintsV3['origin']))
len(np.unique(mintsV2['origin']))
len(np.unique(swapsV3['origin']))
len(np.unique(swapsV2['to']))

# t testing means Mints
summary, results = rp.ttest(group1=FrequenciesMints_BurnsV2['Frequency_x'], group1_name="V2 Mints",
                            group2=FrequenciesMints_Burns['Frequency_x'], group2_name="V3 Mints")
print(results)
# t testing means Burns
summary, results = rp.ttest(group1=FrequenciesMints_BurnsV2['Frequency_y'], group1_name="V2 Burns",
                            group2=FrequenciesMints_Burns['Frequency_y'], group2_name="V3 Burns")
print(results)
# t testing means Swaps
summary, results = rp.ttest(group1=FrequenciesSwap_BurnsV2['Frequency_x'], group1_name="V2 Swaps",
                            group2=FrequenciesSwap_Burns['Frequency_x'], group2_name="V3 Swaps")
print(results)

# Swap Mint ratio plot
plt.plot(FrequenciesSwap_Mints['timestamp'], Freq_RatioSwap_Mints, label='V3', color='mediumorchid')
plt.plot(FrequenciesSwap_MintsV2['timestamp'], Freq_RatioSwap_MintsV2, label='V2', color='deeppink')
plt.xlabel('Date')
plt.ylabel("Swap/Mint Ratio")
plt.title('Number of swaps per mint per day')
plt.legend()
plt.show()

# Swap Burn ratio
plt.plot(FrequenciesSwap_Burns['timestamp'], Freq_RatioSwap_Burns, label='V3', color='mediumorchid')
plt.plot(FrequenciesSwap_BurnsV2['timestamp'], Freq_RatioSwap_BurnsV2, label='V2', color='deeppink')
plt.xlabel('Date')
plt.ylabel("Swap/Burn Ratio")
plt.title('Number of swaps per burn per day')
plt.legend()
plt.show()

# Mint Burn ratio
plt.plot(FrequenciesMints_Burns['timestamp'], Freq_RatioMints_Burns, label='V3', color='mediumorchid')
plt.plot(FrequenciesMints_BurnsV2['timestamp'], Freq_RatioMints_BurnsV2, label='V2', color='deeppink')
plt.xlabel('Date')
plt.ylabel("Mint/Burn Ratio")
plt.title('Number of mints per burn per day')
plt.legend()
plt.show()





######## Market Depth ##########

# Market Depth V3
# Call functions from MarketDepth.py
dfrgns, MintBurn = MarketDepthV3Part1(Pools=Pools, Mints=Mints, Burns=Burns, tickspacing=10, UpperCut=240000,
                                      LowerCut=170000, decimals0=6, decimals1=18)

# Save data after MarketDepthV3Part1 for easy usage
# dfrgns.to_pickle("dfrgnsBIG500!")

# Read Data
dfrgns3000 = pd.read_pickle("dfrgnsBIG3000")
dfrgns500 = pd.read_pickle("dfrgnsBIG500!")

# Calculate Market Depth V3
DepthDistribution = pd.DataFrame()
liqdist500, dfdepth500 = MarketDepthV3Part2(dfrgns500, Pools=Pools, tickspacing=10, depthpct=0.02, decimals0=6,
                                            decimals1=18)
liqdist3000, dfdepth3000 = MarketDepthV3Part2(dfrgns3000, Pools=Pools3000, tickspacing=60, depthpct=0.02, decimals0=6,
                                              decimals1=18)

# MarketDepth V2
PoolsV2 = pools2V2(timestamp_counter=1654041601, pool=ETHUSDCV2)
PoolsV2 = CleanPoolV2(PoolsV2)
# +/- 2% price impact
dfdepthV2 = MarketDepthV2(PoolsV2, 1.02)
dfdepthV2['depth'] = MarketDepthV2(PoolsV2, 1.02).depth + MarketDepthV2(PoolsV2, 0.98).depth
dfdepthV2 = dfdepthV2[0:392]
dfdepthV2 = dfdepthV2.iloc[::-1]  # Reverse dataframe to start with lowest timestamp

# DepthDistribution.loc[6,1] = dfdepthV2['depth'].mean()
# DepthDistribution.loc[6,2] = '+/- 0.5%'
# X_axis = np.arange(len(DepthDistribution.loc[:,2]))
# width = 0.35
# fig, ax = plt.subplots()
# ax.bar(X_axis - width/2, DepthDistribution.loc[:,0]/1000000, width, label = "V3", color = 'mediumorchid')
# ax.tick_params(axis='y', labelcolor='mediumorchid')
# ax2 = ax.twinx()
# ax2.bar(X_axis + width/2, DepthDistribution.loc[:,1]/1000000, width, label = "V2", color = 'deeppink')
# ax2.tick_params(axis='y', labelcolor='deeppink')
# plt.ylim(ymin=0)
# fig.legend()
# plt.xticks(X_axis, DepthDistribution.loc[:,2])
# #plt.xlim(xmin=0)
# plt.title('Mean Market Depth per Price Impact')
# plt.xlabel("+/- % from spot")
# plt.ylabel("Depth ($mm)")
# ax.text(0, DepthDistribution.loc[0,0]/1000000, "x6.95 V2")
# ax.text(1, DepthDistribution.loc[1,0]/1000000, "x10.39 V2")
# ax.text(2, DepthDistribution.loc[2,0]/1000000, "x12.89 V2")
# ax.text(3, DepthDistribution.loc[3,0]/1000000, "x14.07 V2")
# ax.text(4, DepthDistribution.loc[4,0]/1000000, "x14.49 V2")
# ax.text(5, DepthDistribution.loc[5,0]/1000000, "x14.59 V2")
# ax.text(6, DepthDistribution.loc[6,0]/1000000, "x14.60 V2")
# plt.show()

# Merge V2 and V3 Market Depth data to 1 data frame
dfdepthV3 = pd.merge(dfdepth500, dfdepth3000, on=['timestamp'], how="inner")
dfdepthV3['depthV3'] = dfdepthV3['depth_x'] + dfdepthV3['depth_y']
DEPTH = pd.merge(dfdepthV2, dfdepthV3[['timestamp', 'depthV3']], on=['timestamp'], how="inner")

# Plot Market Depth +/-2% Price Impact over time
fig, ax = plt.subplots()
ax.plot(DEPTH['timestamp'], DEPTH['depthV3'] / 1000000, label="V3", color='mediumorchid')
ax.tick_params(axis='y', labelcolor='mediumorchid')
ax2 = ax.twinx()
ax2.plot(DEPTH['timestamp'], DEPTH['depth'] / 1000000, label="V2", color='deeppink')
ax2.tick_params(axis='y', labelcolor='deeppink')
plt.ylim(ymin=0)
fig.legend()
plt.title('Market Depth V2 vs V3 : +/-2% Price Impact ')
plt.xlabel("Date")
plt.ylabel("Depth ($mm)")
plt.show()

# Descriptive Stats Market Depth
((DEPTH['depthV3']) / 1000000).describe()  # In millions of USD
(DEPTH['depth'] / 1000000).describe()  # In millions of USD

# t testing means depth
summary, results = rp.ttest(group1=DEPTH['depth'] / 1000000, group1_name="V2 depth",
                            group2=DEPTH['depthV3'] / 1000000, group2_name="V3 depth")
print(results)

# Correlation V2 and V3
fig, ax = plt.subplots(figsize=(5, 5))
ax.scatter(DEPTH['depth'], DEPTH['depthV3'], s=60, alpha=0.7, edgecolors="k", color='deeppink')
b, a = np.polyfit(DEPTH['depth'], DEPTH['depthV3'], deg=1)
xseq = np.linspace(0, np.max(DEPTH['depth']), num=50)
plt.plot(xseq, a + b * xseq, color="k", lw=2.5)
plt.show()







########### LP Returns ##########

### LP Returns V3 ###

# Load data
Pools3000 = pools2(timestamp_counter=1654041601, pool=ETHUSDCV3_3000)
Burns3000 = burn(timestamp_counter=1654041601, pool=ETHUSDCV3_3000)
Fees3000 = fees(id_counter=99999, pool=ETHUSDCV3_3000)

Pools500 = pools2(timestamp_counter=1654041601, pool=ETHUSDCV3_500)
Burns500 = burn(timestamp_counter=1654041601, pool=ETHUSDCV3_500)
Fees500 = fees(id_counter=99999, pool=ETHUSDCV3_500)

# Clean data
Pools3000 = CleanPool(Pools3000)
Burns3000 = CleanBurns(Burns3000, timeround=True, negativeAmount=True)
Fees3000 = CleanFees(Fees3000)

Pools500 = CleanPool(Pools500)
Burns500 = CleanBurns(Burns500, timeround=True, negativeAmount=True)
Fees500 = CleanFees(Fees500)

# Calculate Returns
Rv3_3000 = LPreturns(Fees3000, Pools3000, Burns3000)
Rv3_500 = LPreturns(Fees500, Pools500, Burns500)
Rv3 = pd.concat([Rv3_3000, Rv3_500], axis=0)

# Delete potential wrong data
Rv3 = Rv3[Rv3['VT'] > 0]
Rv3 = Rv3[Rv3['AdSelCos'] < 0.0000]

# Summarise Results
DesV3 = Rv3.describe()
Rv3['FeeYield'].describe()
Rv3['AdSelCos'].describe()
Rv3['InvHolRet'].describe()

# Plot Return decomposition
plt.hist(Rv3['InvHolRet'], weights=np.ones(len(Rv3['InvHolRet'])) / len(Rv3['InvHolRet']), bins=35, range=(-0.5, 0.5),
         label='Inventory Holding Returns', alpha=1, color="deepskyblue")
plt.hist(Rv3['AdSelCos'], weights=np.ones(len(Rv3['AdSelCos'])) / len(Rv3['AdSelCos']), bins=30, range=(-0.5, 0),
         label='Adverse Selection Costs', alpha=0.7, color="red")
plt.hist(Rv3['FeeYield'], weights=np.ones(len(Rv3['FeeYield'])) / len(Rv3['FeeYield']), bins=30, range=(0, 0.5),
         label='Fee Yield', alpha=0.7, color="lawngreen")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=-0.5, xmax=0.5)
plt.ylim(ymax=1)
plt.ylabel("% LP positions")
plt.title('Liquidity Provider Return Split V3')
plt.legend(title='Return Split V3')
plt.show()

### LP Returns V2 ###
LiqSnaps = LiqSnapsV2(timestamp_counter=1654041601, pool=ETHUSDCV2)
mintsV2 = mintV2(timestamp_counter=1654041601, pool=ETHUSDCV2)
burnsV2 = burnV2(timestamp_counter=1654041601, pool=ETHUSDCV2)
Rv2 = LPreturnsV2(LiqSnaps, burnsV2, mintsV2)

# Delete potential wrong data
Rv2 = Rv2[Rv2['AdSelCos'] < 0.000]
Rv2['F'].describe()
DesV2 = Rv2.describe()

# Plot Returns from just fees
plt.hist(Rv3['FeeYield'], weights=np.ones(len(Rv3['FeeYield'])) / len(Rv3['FeeYield']), bins=57, label='V3', alpha=1,
         color="mediumorchid")
plt.hist(Rv2['F'], weights=np.ones(len(Rv2['F'])) / len(Rv2['F']), bins=30, label='V2', color="deeppink", alpha=0.6)
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=0, xmax=0.3)
plt.ylabel("% LP positions")
plt.title('Liquidity Provider Fee Returns')
plt.legend()
plt.show()

# Plot Return decomposition
plt.hist(Rv2['InvHolRet'], weights=np.ones(len(Rv2['InvHolRet'])) / len(Rv2['InvHolRet']), bins=35, range=(-0.5, 0.5),
         label='Inventory Holding Returns', alpha=1, color="deepskyblue")
plt.hist(Rv2['AdSelCos'], weights=np.ones(len(Rv2['AdSelCos'])) / len(Rv2['AdSelCos']), bins=30, range=(-0.5, 0),
         label='Adverse Selection Costs', alpha=0.7, color="red")
plt.hist(Rv2['F'], weights=np.ones(len(Rv2['F'])) / len(Rv2['F']), bins=30, range=(0, 0.5), label='Fee Yield',
         alpha=0.7, color="lawngreen")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=-0.5, xmax=0.5)
plt.ylim(ymax=1)
plt.ylabel("% LP positions")
plt.title('Liquidity Provider Return Split V2')
plt.legend(title='Return Split V2')
plt.show()

# t testing means feeyields
summary, results = rp.ttest(group1=Rv2['F'], group1_name="V2 Fee Yield",
                            group2=Rv3['FeeYield'], group2_name="V3 Fee Yield")
print(results)

# t testing means AdSelCos
summary, results = rp.ttest(group1=Rv2['AdSelCos'], group1_name="V2 AdSelCos",
                            group2=Rv3['AdSelCos'], group2_name="V3 AdSelCos")
print(results)

# t testing means InvHolRet
summary, results = rp.ttest(group1=Rv2['InvHolRet'], group1_name="V2 InvHolRet",
                            group2=Rv3['InvHolRet'], group2_name="V3 InvHolRet")
print(results)

# t testing means tot
summary, results = rp.ttest(group1=Rv2['Tot'], group1_name="V2 Tot",
                            group2=Rv3['Tot'], group2_name="V3 Tot")
print(results)







### Active vs Passive Strategies ###

# Clean Data
Mints500 = CleanMints(pd.read_pickle("MintsFull500!"), timeround=False)
Burns500 = CleanBurns(pd.read_pickle("BurnsFull500!"), timeround=False, negativeAmount=False)
Mints3000 = CleanMints(pd.read_pickle("MintsFull3000"), timeround=False)
Burns3000 = CleanBurns(pd.read_pickle("BurnsFull3000"), timeround=False, negativeAmount=False)

mintsV3 = pd.concat([Mints500, Mints3000], axis=0)
burnsV3 = pd.concat([Burns500, Burns3000], axis=0)

mintsV2 = mintV2(timestamp_counter=1654041601, end=1620259200, pool=ETHUSDCV2)
burnsV2 = burnV2(timestamp_counter=1654041601, end=1620259200, pool=ETHUSDCV2)
mintsV2 = mintsV2.rename(columns={'to': 'origin'})
burnsV2 = burnsV2.rename(columns={'sender': 'origin'})

# Calculate Durations to link with Returns
DurationV2, originV2 = durationv2(mintsV2, burnsV2)
DurationV3, originV3 = durationv3(mintsV3, burnsV3)

DurationV2 = pd.concat([DurationV2, originV2[['origin', 'liquidity', 'amount0_x', 'amount1_x']]], axis=1)
DurationV3 = pd.concat([DurationV3, originV3[['origin', 'amount', 'amount0_x', 'amount1_x']]], axis=1)

DurationV3 = DurationV3[DurationV3.iloc[:, 0].astype(int) > 0]  # Filter out wrong data

# Calculate Returns
# Load Data
Pools3000 = pools2(timestamp_counter=1654041601, pool=ETHUSDCV3_3000)
Burns3000 = burn(timestamp_counter=1654041601, pool=ETHUSDCV3_3000)
Fees3000 = fees(id_counter=99999, pool=ETHUSDCV3_3000)

Pools500 = pools2(timestamp_counter=1654041601, pool=ETHUSDCV3_500)
Burns500 = burn(timestamp_counter=1654041601, pool=ETHUSDCV3_500)
Fees500 = fees(id_counter=99999, pool=ETHUSDCV3_500)

# Clean data
Pools3000 = CleanPool(Pools3000)
Burns3000 = CleanBurns(Burns3000, timeround=True, negativeAmount=True)
Fees3000 = CleanFees(Fees3000)

Pools500 = CleanPool(Pools500)
Burns500 = CleanBurns(Burns500, timeround=True, negativeAmount=True)
Fees500 = CleanFees(Fees500)

Rv3_3000, D = LPreturns(Fees3000, Pools3000, Burns3000)
Rv3_3000 = pd.concat([Rv3_3000, D['origin']], axis=1)
Rv3_500, E = LPreturns(Fees500, Pools500, Burns500)
Rv3_500 = pd.concat([Rv3_500, E['origin']], axis=1)

Rv3 = pd.concat([Rv3_3000, Rv3_500], axis=0)

# Delete potential wrong calculations
Rv3 = Rv3[Rv3['VT'] > 0]
Rv3 = Rv3[Rv3['AdSelCos'] < 0.0000]

RetDurV3 = pd.merge(Rv3, DurationV3, on='origin', how='inner')
RetDurV3 = RetDurV3.drop_duplicates(keep='first', subset=['origin', 'V0', 'VT', 'F'])

# LP Returns V2
LiqSnaps = LiqSnapsV2(timestamp_counter=1654041601, pool=ETHUSDCV2)
mintsV2 = mintV2(timestamp_counter=1654041601, pool=ETHUSDCV2)
burnsV2 = burnV2(timestamp_counter=1654041601, pool=ETHUSDCV2)

Rv2, B = LPreturnsV2(LiqSnaps, burnsV2, mintsV2)
Rv2 = pd.concat([Rv2, B['origin_x']], axis=1)
Rv2['origin'] = Rv2['origin_x']

RetDurV2 = pd.merge(Rv2, DurationV2, on='origin', how='inner')
RetDurV2 = RetDurV2.drop_duplicates(keep='first', subset=['origin', 'V0', 'VT', 'F'])

RetDurV3.columns.values[9] = "duration"
RetDurV2.columns.values[11] = "duration"

# Active V3 (smallest 25%)
ActiveV3 = RetDurV3.loc[RetDurV3.iloc[:, 9] < RetDurV3.describe().iloc[4, 8]]
# Passive V3 (largest 25%)
PassiveV3 = RetDurV3.loc[RetDurV3.iloc[:, 9] > RetDurV3.describe().iloc[6, 8]]

# Summarise Results V3
ActiveV3Describe = ActiveV3.describe()
PassiveV3Describe = PassiveV3.describe()

# Active V2 (smallest 25%)
ActiveV2 = RetDurV2.loc[RetDurV2.iloc[:, 11] < RetDurV2.describe().iloc[4, 9]]
# Passive V2 (largest 25%)
PassiveV2 = RetDurV2.loc[RetDurV2.iloc[:, 11] > RetDurV2.describe().iloc[6, 9]]

# Summarise Results V2
ActiveV2Describe = ActiveV2.describe()
PassiveV2Describe = PassiveV2.describe()

# t testing means feeyields
summary, results = rp.ttest(group1=ActiveV2['F'], group1_name="V2 Active Fee Yield",
                            group2=ActiveV3['FeeYield'], group2_name="V3 Active Fee Yield")
print(results)

# t testing means feeyields
summary, results = rp.ttest(group1=PassiveV2['F'], group1_name="V2 Passive Fee Yield",
                            group2=PassiveV3['FeeYield'], group2_name="V3 Passive Fee Yield")
print(results)

# t testing means AdSel
summary, results = rp.ttest(group1=ActiveV2['AdSelCos'], group1_name="V2 Active AdSelCos",
                            group2=ActiveV3['AdSelCos'], group2_name="V3 Active AdSelCos")
print(results)

# t testing means AdSel
summary, results = rp.ttest(group1=PassiveV2['AdSelCos'], group1_name="V2 Passive InvHolRet",
                            group2=PassiveV3['AdSelCos'], group2_name="V3 Passive InvHolRet")
print(results)

# t testing means InvHol
summary, results = rp.ttest(group1=ActiveV2['InvHolRet'], group1_name="V2 Active InvHolRet",
                            group2=ActiveV3['InvHolRet'], group2_name="V3 Active InvHolRet")
print(results)

# t testing means InvHol
summary, results = rp.ttest(group1=PassiveV2['InvHolRet'], group1_name="V2 Passive InvHolRet",
                            group2=PassiveV3['InvHolRet'], group2_name="V3 Passive InvHolRet")
print(results)

# t testing means Tot
summary, results = rp.ttest(group1=ActiveV2['Tot'], group1_name="V2 Active Tot",
                            group2=ActiveV3['Tot'], group2_name="V3 Active Tot")
print(results)

# t testing means Tot
summary, results = rp.ttest(group1=PassiveV2['Tot'], group1_name="V2 Passive Tot",
                            group2=PassiveV3['Tot'], group2_name="V3 Passive Tot")
print(results)
