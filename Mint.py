import requests
import pandas as pd
import json
import matplotlib.pyplot as plt
import time


#Use lowercase id for pool data!!!!!!!!!!!!!!!!!!!!
url2 = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
url  = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'


def mint(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8" ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    for i in range(300):
        while True:
            try:

                data = ''' 
                {mints(orderBy: timestamp, orderDirection: desc, first: 100, skip: ''' + str(0) + ''', where:
                 { pool: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", timestamp_lt: ''' + str(timestamp_counter) + '''}
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



def burn(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8" ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    for i in range(300):
        while True:
            try:

                data = ''' 
                {burns(orderBy: timestamp, orderDirection: desc, first: 100, skip: ''' + str(0) + ''', where:
                 { pool: "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8", timestamp_lt: ''' + str(timestamp_counter) + '''}
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


def swaps(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8" ):
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
             { pool: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", timestamp_lt: ''' + str(timestamp_counter) + '''}
            ) {
              pool {
                token0 {
                  id
                  symbol
                  decimals
                }
                token1 {
                  id
                  symbol
                  decimals
                }
              }
                transaction{
                gasUsed
                gasPrice
                blockNumber
              }
              sqrtPriceX96
              amount0
              amount1
              timestamp
              tick
              id
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


        except KeyError as e:
            print(e)
            if e.args[0] != 'data':
                return DATAFRAME2
                break
            elif e.args[0] == 'data':
                print(e)
                time.sleep(0.5)
                continue

def pools2(timestamp_counter = 1654066800, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8" ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    #for i in range(300):
    while True:
            try:

                data = ''' 
                {poolDayDatas(orderBy: date, orderDirection: desc, first: 100, where:
                 { pool: "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8", date_lt: ''' + str(timestamp_counter) + '''}
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

def Fees(id_counter = 99999, pool = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8" ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    id_counter = id_counter
    pool = pool

    while True:
            try:
                data = ''' 
                {positions(first: 1000, orderBy:id,orderDirection: desc, where: {
                        pool: "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8", id_lt: ''' + str(id_counter) + '''}) 
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

                print(DATA["id"])
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


def pools2V2(timestamp_counter = 1654066800, pool = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc" ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    while True:
            try:

                data = ''' 
                {pairDayDatas(orderBy: date, orderDirection: desc, first: 100, where:
                 { pairAddress: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", date_lt: ''' + str(timestamp_counter) + '''}
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


def LiqSnapsV2(timestamp_counter = 1654066800, pool = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc" ):
    DATAFRAME2 = pd.DataFrame()
    DATA2 = pd.DataFrame()
    timestamp_counter = timestamp_counter
    pool = pool

    while True:
            try:

                data = ''' 
                {liquidityPositionSnapshots(orderBy: timestamp, orderDirection: desc, first: 1000, where:
                    {pair: "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc", timestamp_lt: ''' + str(timestamp_counter) + '''}
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










