import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import time

#The Mint.py file is used to gather data from the Uniswap Subgraph API
#Note; Adjusting the pool needs to be done within the function, not in the function command since this doesn't work properly
#Note; The pool addresses needs to be lowercase! Etherscan gives pools in uppercase!


#Entry points for subgraph API, different version different url
url2 = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
url  = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'

#Every function has a built in loop over timestamp/date, since max API request is 1000/100 entries at a time.
#While true loop is used for dealing with too many requests in a short amount of time, if this is the case,
# it waits 0.5 seconds for a new request.


### V3 Data Gathering ####

#mint() needs timestamp_counter (which is max date up to one wants data), and pool adress as input variables
#If random date in the future is given, latest date is current date
#mint() returns blockNumber, origin, timestamp,tickLower,tickUpper,amount,amount0,amount1 from LP provider add liquidity transaction
def mint(timestamp_counter = 1654041601, pool = '"0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    for i in range(300):
        while True:
            try:

                data = ''' 
                {mints(orderBy: timestamp, orderDirection: desc, first: 100, skip: ''' + str(0) + ''', where:
                 { pool: ''' + str(pool) + ''', timestamp_lt: ''' + str(timestamp_counter) + '''}
                ) 
                    {
                      transaction
                      {
                        blockNumber
                      }
                        origin,
                        timestamp,
                        tickLower,
                        tickUpper,
                        amount,
                        amount0,
                        amount1
                    }
                }
                '''

                response = requests.post(url2, json={'query': data})
                DATA = response.content
                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA['data']['mints'])
                DATA.sort_values(by=['timestamp'], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                timestamp_counter = DATA['timestamp'][99]



            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data':

                    print(e)

                    time.sleep(0.5)

                    continue

    return DATAFRAME2


#burn() needs timestamp_counter (which is max date up to one wants data), and pool adress as input variables
#If random date in the future is given, latest date is current date
#burn() returns blockNumber, origin, timestamp,tickLower,tickUpper,amount,amount0,amount1 from LP provider remove liquidity transaction
def burn(timestamp_counter = 1654041601, pool = '"0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    for i in range(300):
        while True:
            try:

                data = ''' 
                {burns(orderBy: timestamp, orderDirection: desc, first: 100, skip: ''' + str(0) + ''', where:
                 { pool: ''' + str(pool) + ''', timestamp_lt: ''' + str(timestamp_counter) + '''}
                ) 
                    {
                      transaction
                      {
                        blockNumber
                      }
                        origin,
                        timestamp,
                        tickLower,
                        tickUpper,
                        amount,
                        amount0,
                        amount1
                    }
                }
                '''

                response = requests.post(url2, json={'query': data})
                DATA = response.content

                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA['data']['burns'])
                DATA.sort_values(by=['timestamp'], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                timestamp_counter = DATA['timestamp'][99]



            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data':

                    print(e)

                    time.sleep(0.5)

                    continue

    return DATAFRAME2


#swaps() needs timestamp_counter (which is max date up to one wants data), and pool adress as input variables
#If random date in the future is given, latest date is current date
#swaps() returns token0id, token0symbol, token0decimals, token1id, token1symbol, token1decimals,
# gasUsed, gasPrice, blockNumber, sqrtPriceX96(Price of the pool after swap as sqrt(token1/token0) Q64.96 value),
# amount0, amount1, timestamp, tick(tick of price), id, origin(Address user) of swap transaction
def swaps(timestamp_counter = 1654041601, end = 1634041601, pool = '"0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    # for i in range(300):
    while True:
        try:
            data = '''
            {
            swaps(orderBy: timestamp, orderDirection: desc, first: 1000, skip: ''' + str(0) + ''', where:
             { pool: ''' + str(pool) + ''', timestamp_lt: ''' + str(timestamp_counter) + '''}
            ) {
              sqrtPriceX96
              timestamp
              origin
             }
            }
            '''

            response = requests.post(url2, json={'query': data})
            DATA = response.content
            DATA = json.loads(DATA)
            DATA = pd.json_normalize(DATA['data']['swaps'])
            DATA.sort_values(by=['timestamp'], inplace=False, ascending=False)

            DATAFRAME2 = pd.concat([DATA2, DATA])
            DATA2 = DATAFRAME2

            timestamp_counter = DATA['timestamp'][999]

            print(timestamp_counter)

            if int(timestamp_counter) < end:
                return DATAFRAME2


        except KeyError as e:
            print(e)
            if e.args[0] != 'data':
                return DATAFRAME2
                break
            elif e.args[0] == 'data':
                print(e)
                time.sleep(5)
                continue

#pools2() needs timestamp_counter (which is max date up to one wants data), and pool adress as input variables
#If random date in the future is given, latest date is current date
#pools2() returns daily agreggated data of specific pool; date, liquidity, sqrtPrice, token0Price, token1Price,
# volumeToken0, volumeToken1, tick
def pools2(timestamp_counter = 1654041601, pool = '"0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    #for i in range(300):
    while True:
            try:

                data = ''' 
                {poolDayDatas(orderBy: date, orderDirection: desc, first: 100, where:
                 { pool: ''' + str(pool) + ''', date_lt: ''' + str(timestamp_counter) + '''}
                ) 
                    {
                        date
                        liquidity
                        sqrtPrice
                        token0Price
                        token1Price
                        volumeToken0
                        volumeToken1
                        tick
                    }
                }
                '''

                response = requests.post(url2, json={'query': data})
                DATA = response.content

                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA["data"]["poolDayDatas"])
                #DATA.sort_values(by=["date"], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                timestamp_counter = DATA["date"][99]

                print(timestamp_counter)

            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data':

                    print(e)

                    time.sleep(0.5)

                    continue

    return DATAFRAME2

#Fees() needs id_counter (which is max id up to one wants data from), and pool adress as input variables
#If LARGE id_counter is given, latest transaction is used
#Fees() returns owner, tickLower{tickIdx}, tickUpper{tickIdx}, depositedToken0, depositedToken1,
# withdrawnToken0,withdrawnToken1, collectedFeesToken0, collectedFeesToken1 of LP provider that has collected his/her fees

def fees(id_counter = 99999, pool = '"0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    id_counter = id_counter
    pool = pool

    while True:
            try:
                data = ''' 
                {positions(first: 1000, orderBy:id,orderDirection: desc, where: {
                        pool: ''' + str(pool) + ''', id_lt: ''' + str(id_counter) + '''}) 
                        {
                        id
                        transaction{
                          timestamp
                        }
                        owner
                        tickLower{tickIdx}
                        tickUpper{tickIdx}
                        depositedToken0
                        depositedToken1
                        withdrawnToken0
                        withdrawnToken1
                        collectedFeesToken0
                        collectedFeesToken1
                      }
                    }
                '''

                response = requests.post(url2, json={'query': data})
                DATA = response.content

                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA["data"]["positions"])
                #DATA.sort_values(by=["date"], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                id_counter = DATA["id"][999]

                print(id_counter)


            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data':

                    print(e)

                    time.sleep(0.5)

                    continue

    return DATAFRAME2

### V2 Data Gathering ####
#pools2V2() needs timestamp_counter (which is max date up to one wants data), and pool adress as input variables
#If random date in the future is given, latest date is current date
#pools2V2() returns daily agreggated data of specific pool; date, reserve0, reserve1, reserveUSD

def pools2V2(timestamp_counter = 1654041601, pool = '"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    while True:
            try:

                data = ''' 
                {pairDayDatas(orderBy: date, orderDirection: desc, first: 100, where:
                 { pairAddress: ''' + str(pool) + ''', date_lt: ''' + str(timestamp_counter) + '''}
                ) 
                    {
                        date
                        reserve0
                        reserve1
                        reserveUSD
                    }
                }
                '''

                response = requests.post(url, json={'query': data})
                DATA = response.content

                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA["data"]["pairDayDatas"])
                #DATA.sort_values(by=["date"], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                timestamp_counter = DATA["date"][99]

                print(timestamp_counter)

            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data':

                    print(e)

                    time.sleep(0.5)

                    continue

    return DATAFRAME2

#LiqSnapsV2() needs timestamp_counter (which is max date up to one wants data), and pool adress as input variables
#If random date in the future is given, latest date is current date
#LiqSnapsV2() returns userId, timestamp, liquidityTokenBalance (amount of liquity tokens minted),
# reseve0, reserve1, liquidityTokenTotalSupply (total amount of liquidity tokens in pool) of LP positions over time
# Data can not be trusted!!!! Seems to return incorrect data.
def LiqSnapsV2(timestamp_counter = 1654041601, pool = '"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    while True:
            try:

                data = ''' 
                {liquidityPositionSnapshots(orderBy: timestamp, orderDirection: desc, first: 1000, where:
                    {pair: ''' + str(pool) + ''', timestamp_lt: ''' + str(timestamp_counter) + '''}
                    )
                    
                    {
                        user
                    {
                        id
                    }
                    timestamp
                    liquidityPosition
                    {
                        liquidityTokenBalance
                    }
                    reserve0
                    reserve1
                    liquidityTokenTotalSupply
                    }
                }
                '''

                response = requests.post(url, json={'query': data})
                DATA = response.content

                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA["data"]["liquidityPositionSnapshots"])
                #DATA.sort_values(by=["date"], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                timestamp_counter = DATA["timestamp"][999]

                print(timestamp_counter)

            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data' and timestamp_counter == 1600401034 :

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data' and timestamp_counter != 1600401034 :

                    print(e)

                    time.sleep(15)

                    continue


    return DATAFRAME2

#mintV2() needs timestamp_counter (which is max date up to one wants data), and pool adress as input variables
#If random date in the future is given, latest date is current date
#mintV2() returns to (who receives LP tokens), sender, timestamp, liquidity, amount0, amount1 from LP provider add liquidity transaction
def mintV2(timestamp_counter = 1654041601, end = 1620259200, pool = '"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    while True:
            try:

                data = ''' 
                {mints(orderBy: timestamp, orderDirection: desc, first: 100, where:
                 { pair: ''' + str(pool) + ''', timestamp_lt: ''' + str(timestamp_counter) + '''}
                ) 
                    {   to,
                        sender,
                        timestamp,
                      	liquidity,
                        amount0,
                        amount1
                    }
                }
                '''

                response = requests.post(url, json={'query': data})
                DATA = response.content

                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA["data"]["mints"])
                #DATA.sort_values(by=["date"], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                timestamp_counter = DATA["timestamp"][99]

                print(timestamp_counter)
                if int(timestamp_counter) < end:
                    return DATAFRAME2

            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data':

                    print(e)

                    time.sleep(0.5)

                    continue

    return DATAFRAME2

#burnV2() needs timestamp_counter (which is max date up to one wants data), and pool adress as input variables
#If random date in the future is given, latest date is current date
#burnV2() returns to, sender(who burns LP tokens), timestamp, liquidity, amount0, amount1 from LP provider remove liquidity transaction
def burnV2(timestamp_counter = 1654041601, end = 1620259200, pool = '"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    while True:
            try:

                data = ''' 
                {burns(orderBy: timestamp, orderDirection: desc, first: 100, where:
                 { pair: ''' + str(pool) + ''', timestamp_lt: ''' + str(timestamp_counter) + '''}
                ) 
                    {
                        to,
                        sender,
                        timestamp,
                      	liquidity,
                        amount0,
                        amount1
                    }
                }
                '''

                response = requests.post(url, json={'query': data})
                DATA = response.content

                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA["data"]["burns"])
                #DATA.sort_values(by=["date"], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                timestamp_counter = DATA["timestamp"][99]

                print(timestamp_counter)

                if int(timestamp_counter) < end:
                    return DATAFRAME2

            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data':

                    print(e)

                    time.sleep(0.5)

                    continue

    return DATAFRAME2




def swapV2(timestamp_counter = 1654041601, end = 1620259200,  pool = '"0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"' ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    while True:
            try:

                data = ''' 
                {swaps(orderBy: timestamp, orderDirection: desc, first: 1000, where:
                 { pair: ''' + str(pool) + ''', timestamp_lt: ''' + str(timestamp_counter) + '''}
                ) 
                    {
                        timestamp,
                    }
                }
                '''

                response = requests.post(url, json={'query': data})
                DATA = response.content

                DATA = json.loads(DATA)
                DATA = pd.json_normalize(DATA["data"]["swaps"])
                #DATA.sort_values(by=["date"], inplace=False, ascending=False)

                DATAFRAME2 = pd.concat([DATA2, DATA])
                DATA2 = DATAFRAME2
                timestamp_counter = DATA["timestamp"][999]

                if int(timestamp_counter) < end:
                    return DATAFRAME2

            except KeyError as e:

                print(e)

                if e.args[0] != 'data':

                    return DATAFRAME2

                    break

                elif e.args[0] == 'data':

                    print(e)

                    time.sleep(10)

                    continue

    return DATAFRAME2


