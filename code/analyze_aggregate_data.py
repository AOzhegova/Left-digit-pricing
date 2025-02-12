
#%%
import duckdb
import pandas as pd
import re
import psutil
import numpy as np
import pyfixest as pf
import gc

pd.options.display.float_format = '{:.2f}'.format  

#%% Load data
# Connect to DuckDB
con = duckdb.connect()

# Define file paths
base_path = "//Sen.valuta.nhh.no/project/Psonr/Data/Aggregate Data/"
months = ["june", "july", "aug", "sep", "oct", "nov"]

# Read the first month's data to determine column structure
sample_query = f"SELECT * FROM read_parquet('{base_path}{months[0]}_data_week_store_sku.parquet') LIMIT 1"
sample_data = con.execute(sample_query).df()
columns = list(sample_data.columns)  # Get column names

#temp = pd.read_parquet(f'{base_path}{months[0]}_data_week_store_sku.parquet')


# Detect column types
columns = list(sample_data.columns)
column_types = sample_data.dtypes.replace({
    'int64': 'BIGINT',
    'float64': 'DOUBLE',
    'object': 'VARCHAR'  # Strings should be VARCHAR, not DOUBLE!
}).to_dict()

# Create merged_data table with the same columns + "ppu"
columns.append("ppu")  # Add "ppu" column from mode data
column_types["ppu"] = "DOUBLE"  # Ensure it's numeric

# Create table dynamically
col_str = ", ".join(f"{col} {column_types[col]}" for col in columns)
create_query = f"CREATE TABLE merged_data ({col_str})"
con.execute(create_query)

# Process each month separately and append to the table
for month in months[:1]:
    # Read data directly in DuckDB
    print(f"Processing {month}...")
    data_query = f"SELECT * FROM read_parquet('{base_path}{month}_data_week_store_sku.parquet')"
    mode_query = f"SELECT week, sku_gtin, store_id, ppu FROM read_parquet('{base_path}{month}_modes_week_store_sku.parquet')"

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

