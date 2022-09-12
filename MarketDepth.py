import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

#The MarketDepth.py file is used to calculate the marketdepth for Uniswap V2 and Uniswap V3 pools.
#Main input variables: #Pooladdress, priceImpact, tickspacing
#Code mainly taken from "The Dominance of Uniswap v3 Liquidity" by Gordon Liao and Dan Robinson.
#Code is in fact the implemantation of Math Appendix 7.1(Derivation for v2 market depth) and 7.2(Derivation for v3 market depth)

#V2 depth should be 20x/30x lower than V3 according to Adams (researcher at Uniswap)


### Appendix 7.1(Derivation for v2 market depth) ####
def MarketDepthV2(Pools, priceImpact):
    priceImpact = priceImpact
    Pools = Pools
    MarketDepth = pd.DataFrame()

    Pools['liquidity'] = np.sqrt(Pools['reserve0']*Pools['reserve1'])
    B = np.abs( (Pools['liquidity']/np.sqrt((priceImpact)*Pools['Prices']) ) - (Pools['liquidity']  / np.sqrt(Pools['Prices'])) )
    C = B
    MarketDepth["depth"] = C

    MarketDepth = pd.concat([Pools['timestamp'], MarketDepth["depth"]], axis = 1)

    return MarketDepth


### Appendix 7.1(Derivation for v3 market depth) ####

# Code mainly from "The Dominance of Uniswap v3 Liquidity" by Gordon Liao and Dan Robinson
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


# Created to easily use/call the functions listed above
# MarketDepthV3Part1() takes LONGGGGGGGGGGG to run. Safe file directly afterwards is preferred
def MarketDepthV3Part1(Pools, Mints, Burns, tickspacing=60, decimals0 = 6, decimals1 = 18, UpperCut = 240000, LowerCut = 170000):
    Pools = Pools
    Mints = Mints
    Burns = Burns

    # Lower and Upper cut used to speed up reduce complexity algorithm. Not much liquidity is found outside these bounds, so it doesn't hurt.
    MintBurn = pd.concat([Mints, Burns], axis=0)
    MintBurn = MintBurn[MintBurn.tickLower > LowerCut]
    MintBurn = MintBurn[MintBurn.tickLower < UpperCut]

    dfrgns = genLiqRangeOverTime(MintBurn, tickspacing)

    return dfrgns, MintBurn

#MarketDepthV3Part2() easy to run. Safe file directly afterwards is not necessary
def MarketDepthV3Part2(dfrgns, Pools, depthpct=0.02, tickspacing=60, decimals0 = 6, decimals1 = 18):
    depthpct = depthpct
    tickspacing = tickspacing
    dfrgns = dfrgns

    #C lean Data
    dfprice = Pools[['timestamp', 'token0Price']].rename(columns={"token0Price": "P"})
    dfprice['P'] = dfprice['P'].astype(float)
    dfrgns = dfrgns.reset_index()
    dfrgns = pd.merge(dfrgns, dfprice, on="timestamp")

    dfrgns['amount'] = dfrgns['amount'] / pow(10, 6 - 18) #transfer back to non-cleaned data

    # Calculate Market Depth over time
    liqdist = pd.DataFrame(genDepthOverTime(dfrgns, tickspacing=tickspacing))
    dfdepth = fillGranularDistributionOverTime(liqdist, depthpct=depthpct, tickspacing=tickspacing)

    return liqdist, dfdepth