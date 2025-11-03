"""Make data for studying the intensity of left-digit pricing and competition
"""

#%%
import duckdb
import pandas as pd
import re
import psutil
import numpy as np
import pyfixest as pf
import gc
import matplotlib.pyplot as plt
from pathlib import Path
import geopandas as gpd
import statsmodels.formula.api as smf

pd.options.display.float_format = '{:.2f}'.format  
pd.set_option('display.max_columns', None)  # Show all columns in DataFrame output

current_dir = Path(__file__)
gitfolder = current_dir.parent.parent
datafolder = current_dir.parent.parent.parent / 'data'
output = gitfolder / 'output' 

#%% Load data
# Connect to DuckDB
con = duckdb.connect()

# Define file paths
base_path = datafolder / "Aggregate Data"
months = ["june", "july", "aug", "sep", "oct", "nov"]

# Read the first month's data to determine column structure
sample_query = f"SELECT * FROM read_parquet('{base_path / (months[0] + '_data_week_store_sku.parquet')}') LIMIT 1"
sample_data = con.execute(sample_query).df()

# Detect column types
column_types = sample_data.dtypes.map(str).replace({
    'int64': 'BIGINT',
    'float64': 'DOUBLE',
    'object': 'VARCHAR'
}).to_dict()


# Create merged_data table with the same columns + "ppu"
columns = list(sample_data.columns) + ["ppu"]
column_types["ppu"] = "DOUBLE"

# Create table dynamically
col_str = ", ".join(f"{col} {column_types[col]}" for col in columns)
create_query = f"CREATE TABLE merged_data ({col_str})"
con.execute(create_query)

# Process each month separately and append to the table
for month in months[:1]:
    # Read data directly in DuckDB
    print(f"Processing {month}...")
    data_query = f"SELECT * FROM read_parquet('{base_path / (month + '_data_week_store_sku.parquet')}')"
    mode_query = f"SELECT week, sku_gtin, store_id, ppu FROM read_parquet('{base_path / (month + '_modes_week_store_sku.parquet')}')"

    # Merge inside DuckDB, ensuring column match
    merge_query = f"""
        INSERT INTO merged_data ({', '.join(columns)})
        SELECT d.*, m.ppu 
        FROM ({data_query}) AS d
        LEFT JOIN ({mode_query}) AS m 
        USING (week, sku_gtin, store_id)
    """
    
    con.execute(merge_query)
    print(f"Processed {month}")

df_full = con.execute("SELECT * FROM merged_data").df()
# %%
df_subset = df_full[(df_full['price'] > 0) & (df_full['price'] < 1000) & (df_full['quantity'] > 0) &  (df_full['quantity'] < 201)]


df_subset.store_id = df_subset.store_id.astype('category')
df_subset.kjedeid = df_subset.kjedeid.astype('category')
df_subset.sku_gtin = df_subset.sku_gtin.astype('category')
df_subset.week = df_subset.week.astype('category')

chain_format_mapping = {'Extra': 'discounter', 'Prix': 'discounter', 'kiwi': 'discounter', 'Rema': 'discounter', 'meny': 'supermarket', 'spar': 'supermarket', 'Mega': 'supermarket', 'Obs': 'hypermarket', 'joker': 'convenience', 'Marked': 'convenience', 'Matkroken': 'convenience', 'nærbutikken': 'convenience'}

df_subset['format'] = df_subset.kjedeid.map(chain_format_mapping)

#%% Define left-digit pricing
def krone_ends_with_nine(price):
    """
    Checks if the integer part (kroner) of the price ends with 9, 
    but not if the integer part is >= 90.
    """
    kroner_part = int(price)
    return kroner_part % 10 == 9 and kroner_part < 90

def ore_ends_with_nine(price):
    """
    Checks if the decimal part (øre) is exactly 0.90, 0.95, or 0.99.
    Also accepts 0.9 as 0.90.
    """
    ore = round(price * 100) % 100  # Extract øre as an integer (e.g. 12.95 -> 95)
    return ore in [90, 95, 99]

#%%
df_subset.loc[:, 'krone_ends_with_nine'] = df_subset['ppu'].apply(krone_ends_with_nine)
df_subset.loc[:, 'ore_ends_with_nine'] = df_subset['ppu'].apply(ore_ends_with_nine)
df_subset.loc[:, 'ends_with_nine'] = df_subset['krone_ends_with_nine'] | df_subset['ore_ends_with_nine']
df_subset.store_id = df_subset.store_id.astype('int64')   
df_store = df_subset.groupby('store_id', as_index=False).agg({
    'ends_with_nine': 'mean',
    'krone_ends_with_nine': 'mean',
    'ore_ends_with_nine': 'mean',
    'trans': 'sum',
    'quantity': 'sum'
})
#df_store.to_parquet("M:/df_store.parquet", index=False)

#%%
#%% # Load geodata, keys: store id
stores = gpd.read_file(r'M:\grocery_server\data\geodata\NARING_Dagligvare.gdb')
stores['id_clean'] = stores['id'].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
stores['id_int'] = stores['id_clean'].astype(float).astype('int64')
geodata_df = stores.loc[:,['id_int', 'kjedeid','paraplykjede','oms','geometry']].drop_duplicates()

# Merge store-level data with geodata
df_store = df_store.merge(geodata_df, left_on='store_id', right_on='id_int', how='left')

# Check how many stores did not match in Geodata
print('Number of stores without Geodata:', df_store.kjedeid.isna().sum())
print('Number of stores with Geodata:', df_store.kjedeid.notna().sum())