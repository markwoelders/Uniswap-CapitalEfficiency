import numpy as np
import pandas as pd

def ratiosV3(Swaps, Mints, Burns):
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

    return FrequenciesSwap_Mints, FrequenciesSwap_Burns,FrequenciesMints_Burns, Freq_RatioSwap_Mints, Freq_RatioSwap_Burns, Freq_RatioMints_Burns

def ratiosV2(swapsV2, mintsV2, burnsV2):
    swapsV2['timestamp'] = swapsV2['timestamp'].round('D')
    mintsV2['timestamp'] = mintsV2['timestamp'].round('D')
    burnsV2['timestamp'] = burnsV2['timestamp'].round('D')

    SwapsFreq = pd.DataFrame()
    MintsFreq = pd.DataFrame()
    BurnsFreq = pd.DataFrame()

    SwapsFreq['Frequency'] = swapsV2.groupby(['timestamp']).size()
    MintsFreq['Frequency'] = mintsV2.groupby(['timestamp']).size()
    BurnsFreq['Frequency'] = burnsV2.groupby(['timestamp']).size()

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

    return FrequenciesSwap_Mints, FrequenciesSwap_Burns,FrequenciesMints_Burns, Freq_RatioSwap_Mints, Freq_RatioSwap_Burns, Freq_RatioMints_Burns
