
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

pd.options.display.float_format = '{:.2f}'.format  

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
for month in months[:2]:
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

df = con.execute("SELECT * FROM merged_data").df()

# %%
df = df[(df['price'] > 0) & (df['price'] < 1000) & (df['quantity'] > 0) &   (df['quantity'] < 201)]

df.store_id = df.store_id.astype('category')
df.kjedeid = df.kjedeid.astype('category')
df.sku_gtin = df.sku_gtin.astype('category')
df.week = df.week.astype('category')

#%%
def krone_ends_with_nine(price):
    '''
    The function splits the price by the decimal point and
    checks if the integer part (kroner) ends with 9.
    '''
    # Convert the price to string
    price_str = str(price)
    
    # Split the price by the decimal point
    parts = price_str.split('.')
    
    # Check if the kroner part (integer part) ends with 9
    kroner_part = parts[0] if len(parts) > 0 else ''
    pattern = r'\d*9$'
    
    match = re.search(pattern, kroner_part)
    
    return match is not None

def ore_ends_with_nine(price):
    '''
    The function splits the price by the decimal point and
    checks if the decimal part (øre) ends with 9.
    '''
    # Convert the price to string
    price_str = str(price)
    
    # Split the price by the decimal point
    parts = price_str.split('.')
    
    # Check if the øre part (decimal part) ends with 9
    ore_part = parts[1] if len(parts) > 1 else ''
    pattern = r'9$|90$|95$'
    
    match = re.search(pattern, ore_part)
    
    return match is not None
# %%
df.loc[:, 'krone_ends_with_nine'] = df['ppu'].apply(krone_ends_with_nine)
df.loc[:, 'ore_ends_with_nine'] = df['ppu'].apply(ore_ends_with_nine)
df.loc[:, 'ends_with_nine'] = df['krone_ends_with_nine'] | df['ore_ends_with_nine']

# Add necessary columns for regression
df['log_price'] = np.log(df['ppu'])
df['log_quantity'] = np.log(df['quantity'])
df['log_trans'] = np.log(df['trans'])
df['log_price_krone'] = df.log_price * df.krone_ends_with_nine
df['log_price_ore'] = df.log_price * df.ore_ends_with_nine


#%% Descriptive statistics on the usage of prices ending with 9
store_df = df.groupby(['store_id','kjedeid'], as_index=False).agg({'ends_with_nine': 'mean', 'krone_ends_with_nine': 'mean', 'ore_ends_with_nine': 'mean'})

#%% Plot a hist of 9-endings frequences
plt.hist(store_df.krone_ends_with_nine, bins=60, color='slateblue', alpha=0.8, edgecolor='black', label='At krone-level')
plt.hist(store_df.ore_ends_with_nine, bins=60, color='skyblue', alpha=0.9, edgecolor='black', label='At øre-level')

average_krone = store_df.krone_ends_with_nine.mean()
average_ore = store_df.ore_ends_with_nine.mean()

# Adding labels and title for clarity
plt.xlabel('Stores')
plt.ylabel('Frequency of Prices with 9-endings')
plt.title('Distribution of Stores with Left-digit Pricing')
plt.legend(loc='upper right')

# Display the plot
plt.savefig(f'{output}/dist_9endings_allstores.png')

#%% Hist for discounters
discounters = ['Extra', 'Prix', 'kiwi', 'Rema']

# Create subplots
fig, axs = plt.subplots(1, 4, figsize=(20, 5))
axs = axs.flatten()

# Plot histograms for each kjede
for ax, kjede_id in zip(axs, discounters):
    subset = store_df[store_df.kjedeid == kjede_id]
    
    # Plot krone_ends_with_nine
    ax.hist(subset.krone_ends_with_nine, bins=30, color='#ffd166', edgecolor='black', alpha=0.9, label='At krone-level')
    
    # Plot ore_ends_with_nine
    ax.hist(subset.ore_ends_with_nine, bins=30, color='#073b4c', edgecolor='black', alpha=0.7, label='At øre-lvel')
    
    ax.set_xlabel('Frequence of Left-digit Pricing')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Distribution of Left-digit Pricing for {kjede_id}')
    ax.legend()

# Adjust layout
plt.tight_layout()
#plt.savefig(f'{output}/dist_9endings_discounter.png')

#%% Hist for convenience stores
convenience = ['joker','Marked','Matkroken','nærbutikken']

# Create subplots
fig, axs = plt.subplots(1, 4, figsize=(20, 5))
axs = axs.flatten()

# Plot histograms for each kjede
for ax, kjede_id in zip(axs, convenience):
    subset = store_df[store_df.kjedeid == kjede_id]
    
    # Plot krone_ends_with_nine
    ax.hist(subset.krone_ends_with_nine, bins=30, color='#ffd166', edgecolor='black', alpha=0.9, label='At krone-level')
    
    # Plot ore_ends_with_nine
    ax.hist(subset.ore_ends_with_nine, bins=30, color='#073b4c', edgecolor='black', alpha=0.7, label='At øre-lvel')
    
    ax.set_xlabel('Frequence of Left-digit Pricing')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Distribution of Left-digit Pricing for {kjede_id}')
    ax.legend()

# Adjust layout
plt.tight_layout()
plt.savefig(f'{output}/dist_9endings_convenience.png')

#%% Hist for supermarkets
supermarkets = ['Mega','spar','meny','Obs']

# Create subplots
fig, axs = plt.subplots(1, 4, figsize=(20, 5))
axs = axs.flatten()

# Plot histograms for each kjede
for ax, kjede_id in zip(axs, supermarkets):
    subset = store_df[store_df.kjedeid == kjede_id]
    
    # Plot krone_ends_with_nine
    ax.hist(subset.krone_ends_with_nine, bins=30, color='#ffd166', edgecolor='black', alpha=0.9, label='At krone-level')
    
    # Plot ore_ends_with_nine
    ax.hist(subset.ore_ends_with_nine, bins=30, color='#073b4c', edgecolor='black', alpha=0.7, label='At øre-lvel')
    
    ax.set_xlabel('Frequence of Left-digit Pricing')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Distribution of Left-digit Pricing for {kjede_id}')
    ax.legend()

# Adjust layout
plt.tight_layout()
plt.savefig(f'{output}/dist_9endings_sup.png')

#%%
store_df.groupby('kjedeid').agg({'krone_ends_with_nine': 'mean', 'ore_ends_with_nine': 'mean'})

#%% Check memory usage to avoid memory issues
memory = psutil.virtual_memory()

print(f"Total memory: {memory.total / (1024**3):.2f} GB")
print(f"Available memory: {memory.available / (1024**3):.2f} GB")
print(f"Used memory: {memory.used / (1024**3):.2f} GB")
print(f"Memory usage: {memory.percent}%")

#%% MODELS for log quantity
model_q = pf.feols('log_quantity ~ log_price + krone_ends_with_nine + ore_ends_with_nine | sku_gtin + store_id', data=df)
model_q.summary()
del model_q
gc.collect()

model_q_interact = pf.feols('log_quantity ~ log_price + krone_ends_with_nine + log_price_krone | sku_gtin + store_id', data=df)
model_q_interact.summary()
del model_q_interact
gc.collect()

#%% MODELS for log transactions
model_trans = pf.feols('log_trans ~ log_price + krone_ends_with_nine + ore_ends_with_nine | sku_gtin + store_id', data=df, vcov = {'CRV1':'sku_gtin + store_id'})
model_trans.summary()
del model_trans
gc.collect()

model_trans_interact = pf.feols('log_trans ~ log_price + ore_ends_with_nine + log_price_ore | sku_gtin + store_id', data=df)
model_trans_interact.summary()
del model_trans_interact
gc.collect()

model_full = pf.feols('log_trans ~ log_price + krone_ends_with_nine + ore_ends_with_nine + log_price_krone + log_price_ore | sku_gtin + store_id + week', data=df)
model_full.summary()
del model_full
gc.collect()

#%%
def demean_df(df, col, group_cols):
    """
    Function to demean data by group.
    Parameters:
    - df: DataFrame
    - col: Column to demean
    - group_cols: List of columns or a single column to group by
    Returns:
    - Demeaned column
    """
    return df[col] - df.groupby(group_cols)[col].transform('mean')

# %%
# SIMPLE FIXED EFFECTS: Demean data by store AND product
df['demeaned_logquantity_by_store'] = demean_df(df, col='log_quantity', group_cols=['store_id'])
df['demeaned_log_price_by_store'] = demean_df(df, col='log_price', group_cols=['retail_store_id'])
df_filtered['demeaned_log_quantity_by_store'] = demean_df(df_filtered, col='log_quantity', group_cols=['retail_store_id'])
df_filtered['demeaned_krone_ends_with_nine_by_store'] = demean_df(df_filtered, col='krone_ends_with_nine', group_cols=['retail_store_id'])
df_filtered['demeaned_ore_ends_with_nine_by_store'] = demean_df(df_filtered, col='ore_ends_with_nine', group_cols=['retail_store_id'])

df_filtered['demeaned_log_extended_amount'] = demean_df(df_filtered, col='demeaned_log_extended_amount_by_store', group_cols=['gtin'])
df_filtered['demeaned_log_price'] = demean_df(df_filtered, col='demeaned_log_price_by_store', group_cols=['gtin'])
df_filtered['demeaned_log_quantity'] = demean_df(df_filtered, col='demeaned_log_quantity_by_store', group_cols=['gtin'])
df_filtered['demeaned_krone_ends_with_nine'] = demean_df(df_filtered, col='demeaned_krone_ends_with_nine_by_store', group_cols=['gtin'])
df_filtered['demeaned_ore_ends_with_nine'] = demean_df(df_filtered, col='demeaned_ore_ends_with_nine_by_store', group_cols=['gtin'])

# Add interaction terms
df_filtered['demeaned_krone_price'] = df_filtered['demeaned_krone_ends_with_nine'] * df_filtered['demeaned_log_price']
df_filtered['demeaned_ore_price'] = df_filtered['demeaned_ore_ends_with_nine'] * df_filtered['demeaned_log_price']
