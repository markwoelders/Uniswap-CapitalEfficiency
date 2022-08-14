import numpy as np
import pandas as pd

def durationv3(Mints, Burns):
    A = pd.merge(Mints, Burns, on=["origin", "tickLower", "tickUpper", "amount"], how = "inner")
    #Fill na with max date since these mint transactions have no corresponding burns yet, aka liquidity still avtive
    A["timestamp_y"]= A["timestamp_y"].fillna(np.max(Burns["timestamp"]))

    B = A["timestamp_y"] - A["timestamp_x"]

    return B

def durationv2(mintsV2, burnsV2):
    mintsV2 = mintsV2.rename(columns={'to': 'origin'})
    burnsV2 = burnsV2.rename(columns={'sender': 'origin'})

    mintsV2['timestamp'] = pd.to_datetime(mintsV2['timestamp'], unit='s')
    burnsV2['timestamp'] = pd.to_datetime(burnsV2['timestamp'], unit='s')

    A = pd.merge(mintsV2, burnsV2, on=["origin", "liquidity"], how="inner")

    # Fill na with max date since these mint transactions have no corresponding burns yet, aka liquidity still active
    A["timestamp_y"] = A["timestamp_y"].fillna(np.max(burnsV2["timestamp"]))

    B = A["timestamp_y"] - A["timestamp_x"]

    return B

