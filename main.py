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
Swaps = swaps(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Pools = pools2(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Mints = mint(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Burns = burn(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)

#Save Data
# Swaps.to_pickle("SwapsFull")
# Pools.to_pickle("PoolsFull500")
# Mints.to_pickle("MintsFull500")
# Burns.to_pickle("BurnsFull500")

#Read Data
Swaps = pd.read_pickle("SwapsFull")
Pools = pd.read_pickle("PoolsFull500")
Mints = pd.read_pickle("MintsFull500")
Burns = pd.read_pickle("BurnsFull500")

#Clean Data
Swaps = CleanSwaps(Swaps)
Pools = CleanPool(Pools)
Mints = CleanMints(Mints, timeround=True)
Burns = CleanBurns(Burns, timeround=True, negativeAmount=True)


######## Duration Liquidity Positions
# Note data should contain nonrounded timestamp
Mints = CleanMints(pd.read_pickle("MintsFull500"), timeround=False)
Burns = CleanBurns(pd.read_pickle("BurnsFull500"), timeround=False, negativeAmount=False)
DurationV3_500 = durationv3(Mints, Burns)
Mints = CleanMints(pd.read_pickle("MintsFull3000"), timeround=False)
Burns = CleanBurns(pd.read_pickle("BurnsFull3000"), timeround=False, negativeAmount=False)
DurationV3_3000 = durationv3(Mints, Burns)

mintsV2 = mintV2(timestamp_counter = 1654041601, pool = ETHUSDCV2 )
burnsV2 = burnV2(timestamp_counter = 1654041601, pool = ETHUSDCV2 )
mintsV2 = mintsV2.rename(columns={'to': 'origin'})
burnsV2 = burnsV2.rename(columns={'sender': 'origin'})

DurationV2 = durationv2(mintsV2, burnsV2)

plt.hist(DurationV2.astype('timedelta64[h]'), bins = 100, cumulative=False)
DurationV2.describe()

#Issue: We assume now that all liquidity is burned at the maximum date of the dataset, however, that actually has not happened!
#Therefore, we might underestimate the actual duration of liquidity positions.



######## Swap burn ratio's V3
swapsV2 = swapV2(timestamp_counter = 1654041601, pool = '"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"' )

FrequenciesSwap_Mints, FrequenciesSwap_Burns, FrequenciesMints_Burns, Freq_RatioSwap_Mints, Freq_RatioSwap_Burns, Freq_RatioMints_Burns  = ratiosV3(Swaps, Mints, Burns)

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



######## Market Depth convert to something daily

# Call functions from other file
dfrgns, MintBurn = MarketDepthV3Part1(Pools = Pools, Mints = Mints, Burns = Burns, tickspacing=10, UpperCut=240000, LowerCut=170000,  decimals0 = 6, decimals1 = 18)

#save data after MarketDepthV3Part1
#dfrgns.to_pickle("dfrgnsBIG500!")
dfrgns3000 = pd.read_pickle("dfrgnsBIG3000")
dfrgns500 = pd.read_pickle("dfrgnsBIG500!")

#MarketDepthV3Part2
liqdist500, dfdepth500 = MarketDepthV3Part2(dfrgns500, Pools=Pools, tickspacing=10, depthpct=0.02,  decimals0 = 6, decimals1 = 18)
liqdist3000, dfdepth3000 = MarketDepthV3Part2(dfrgns3000, Pools=Pools, tickspacing=60, depthpct=0.02,  decimals0 = 6, decimals1 = 18)


plt.plot(dfdepth500['timestamp'],  dfdepth500['depth'][0:391] + dfdepth3000['depth'][0:391] )
plt.plot(dfdepth3000['timestamp'],  dfdepth3000['depth'] )

#MarketDepth V2
PoolsV2 = pools2V2(timestamp_counter = 1654041601, pool = ETHUSDCV2 )
PoolsV2 = CleanPoolV2(PoolsV2)

#+/- 2% price impact
dfdepthV2 = MarketDepthV2(PoolsV2, 1.02)
dfdepthV2['depth'] = MarketDepthV2(PoolsV2, 1.02).depth + MarketDepthV2(PoolsV2, 0.98).depth
dfdepthV2 = dfdepthV2[0:391]

#Plot V2 and V3 depth. Note: V3 pools are aggregated. Is that fair comparison?
#plt.plot(dfdepth500['timestamp'], dfdepth3000['depth'][0:391] ,label = "V3 USDC-ETH 3000")
#plt.plot(dfdepth500['timestamp'], dfdepth500['depth'][0:391] ,label = "V3 USDC-ETH 500")
plt.plot(dfdepth500['timestamp'], dfdepth500['depth'][0:391] + dfdepth3000['depth'][0:391] ,label = "V3 USDC-ETH")
plt.plot(dfdepthV2['timestamp'], 20*dfdepthV2['depth'], label = "V2 USDC-ETH")
plt.xlabel('Timestamp')
plt.ylabel("Depth $")
plt.xlim(xmin=dfdepth500.iloc[50,0])
#plt.ylim(ymax=250e6, ymin = 0)
plt.legend(title='V2/V3')
plt.title('Market Depth V2 vs V3 : USDC-ETH +/-2% Price Impact ')
plt.show()

plt.plot(PoolsV2['timestamp'], PoolsV2['Prices'])

np.max(dfdepthV2)
np.max(PoolsV2)


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
Pools = pools2(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Burns = burn(timestamp_counter = 1654041601, pool = ETHUSDCV3_3000)
Fees = fees(id_counter = 99999, pool = ETHUSDCV3_3000 )

#Clean data
Pools = CleanPool(Pools)
Burns = CleanBurns(Burns)
Fees = CleanFees(Fees)

#Calculate Returns
Rv3 = LPreturns(Fees, Pools, Burns)
Rv3['FeeYield'].describe()

#Plot Returns from just fees
plt.hist(Rv3['FeeYield'], bins = 50, color = "pink")
plt.xlabel('Returns')
plt.ylabel("Number closed LP positions")
plt.title('Liquidity Provider Returns V3')
plt.show()

#Plot Return decomposition
plt.hist(Rv3['InvHolRet'], weights=np.ones(len(Rv3['InvHolRet'])) / len(Rv3['InvHolRet']), bins = 50, label = 'Inventory Holding Returns', alpha = 0.7, color = "pink")
plt.hist(Rv3['AdSelCos'], weights=np.ones(len(Rv3['AdSelCos'])) / len(Rv3['AdSelCos']), bins = 50 , label = 'Adverse Selection Costs', alpha=0.5, color = "purple")
plt.hist(Rv3['FeeYield'], weights=np.ones(len(Rv3['FeeYield'])) / len(Rv3['FeeYield']), bins = 50, label = 'Fee Yield', alpha = 0.5, color = "green")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=-1, xmax = 1)
plt.ylabel("% closed LP positions")
plt.title('Liquidity Provider Return splits')
plt.legend(title='Return Split')
plt.show()


#LP Returns V2
LiqSnaps = LiqSnapsV2(timestamp_counter = 1654041601, pool = ETHUSDCV2 )
mintsV2 = mintV2(timestamp_counter = 1654041601, pool = ETHUSDCV2 )
burnsV2 = burnV2(timestamp_counter = 1654041601, pool = ETHUSDCV2)
Rv2 = LPreturnsV2(LiqSnaps, burnsV2, mintsV2)

Rv2['F'].describe()

#Plot Returns from just fees
plt.hist(Rv2['F'], weights=np.ones(len(Rv2['F'])) / len(Rv2['F']), bins = 100, label = 'v2', color = "green", alpha =0.2, density=False)
plt.hist(Rv3['FeeYield'], weights=np.ones(len(Rv3['FeeYield'])) / len(Rv3['FeeYield']), bins = 25, label = 'v3', color = "purple", alpha = 0.5, density=False)
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=0, xmax = 1)
plt.ylabel("% closed LP positions")
plt.title('Liquidity Provider Fee Returns')
plt.legend(title='Fee returns')
plt.show()

#Plot Return decomposition
plt.hist(Rv2['InvHolRet'], weights=np.ones(len(Rv2['InvHolRet'])) / len(Rv2['InvHolRet']), bins = 50, label = 'Inventory Holding Returns', alpha = 0.7, color = "pink")
plt.hist(Rv2['AdSelCos'], weights=np.ones(len(Rv2['AdSelCos'])) / len(Rv2['AdSelCos']), bins = 50 , label = 'Adverse Selection Costs', alpha=0.5, color = "purple")
plt.hist(Rv2['F'], weights=np.ones(len(Rv2['F'])) / len(Rv2['F']), bins = 50, label = 'Fee Yield', alpha = 0.5, color = "green")
plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
plt.xlabel('Returns')
plt.xlim(xmin=-1, xmax = 1)
plt.ylabel("% closed LP positions")
plt.title('Liquidity Provider Return splits')
plt.legend(title='Return Split')
plt.show()

Rv2['InvHolRet'].describe()   #v2 prob lower because of duration LP position is bigger
Rv3['InvHolRet'].describe()