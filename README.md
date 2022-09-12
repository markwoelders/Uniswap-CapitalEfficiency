# Uniswap-CapitalEfficiency

### Mint.py

The Mint.py file is used to gather data from the Uniswap Subgraph API
Functions need timestamp_counter (which is max date up to one wants data), and pool adress as input variables.
Functions return data on Burns, Mints, Pools, Fees

### DataCleaning.py

DataCleaning.py file is used to clean the data from the Mint.Py file.
Functions need specific dataset from Mint.py as input
Functions return cleaned datasets


### LP.py
Lp.py file is used to calculate LP returns for Uniswap V2 and Uniswap V3
Functions need datasets from  DataCleaning.py as input, or sometimes directly from Mint.py
Functions return LP returns


### Ratios.py
Ratios.py file is used to calculate daily frequencies and ratios for Uniswap V2 and Uniswap V3
Functions need datasets from  DataCleaning.py as input
Functions return daily frequencies and ratios


### LPDuration.py
LPDuration.py file is used to calculate duration of liquidity positions for Uniswap V2 and Uniswap V3
Functions need Mints and Burns datasets from  DataCleaning.py as input
Functions return duration of liquidity positions


### MarketDepth.py
The MarketDepth.py file is used to calculate the marketdepth for Uniswap V2 and Uniswap V3 pools.
Main input variables: Pooladdress, priceImpact, tickspacing
Code mainly taken from "The Dominance of Uniswap v3 Liquidity" by Gordon Liao and Dan Robinson.
Code is in fact the implemantation of Math Appendix 7.1(Derivation for v2 market depth) and 7.2(Derivation for v3 market depth)
Functions returns market depth over time for Uniswap V2 and Uniswap V3


### PoolContracts.py
The PoolContracts.py file is used to create a list of pool contract ids and corresponding tickspacing and convert them to lower case
This file should be seen as notes, but can be implemented as more pools will be analysed

### Main.py 
Main.py file is used to call the functions from the other files and structure and visualize the output





