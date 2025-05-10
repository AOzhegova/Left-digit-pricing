"""
Here I run regressions to study whether left-digit pricing reduces the price elasticity of demand.
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
for month in months:
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

df = df_subset.sample(frac=0.1, random_state=42).copy()
del df_full, df_subset
gc.collect()

df.store_id = df.store_id.astype('category')
df.kjedeid = df.kjedeid.astype('category')
df.sku_gtin = df.sku_gtin.astype('category')
df.week = df.week.astype('category')

#%% Assign chains to formats
chain_format_mapping = {'Extra': 'discounter', 'Prix': 'discounter', 'kiwi': 'discounter', 'Rema': 'discounter', 'meny': 'supermarket', 'spar': 'supermarket', 'Mega': 'supermarket', 'Obs': 'hypermarket', 'joker': 'convenience', 'Marked': 'convenience', 'Matkroken': 'convenience', 'nærbutikken': 'convenience'}

df['format'] = df.kjedeid.map(chain_format_mapping)

#%% # Load geodata, keys: store id
stores = gpd.read_file(r'M:\grocery_server\data\geodata\NARING_Dagligvare.gdb')
stores['id_clean'] = stores['id'].apply(lambda x: re.sub(r'[^0-9]', '', str(x)))
stores['id_int'] = stores['id_clean'].astype(float).astype('int64')
stores_subset = stores.loc[:,['id_int','kommune_id']].drop_duplicates()
df.store_id = df.store_id.astype('int64')   

del stores, stores_subset
gc.collect()

df = df.merge(stores_subset[['id_int', 'kommune_id']], left_on='store_id', right_on='id_int', how='left')
df.loc[df.kommune_id.isna(), 'kommune_id'] = '0301'  # Assign a default value for missing kommune_id

# for each product calculate an average price per week in other stores of the same chain in other municipalities
df['price_iv'] = df.groupby(['sku_gtin', 'week', 'kjedeid'], as_index=False)['ppu'].transform(lambda x: x.mean())

#%%
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
df['log_price_iv'] = np.log(df['price_iv'])
#%% Check memory usage to avoid memory issues
memory = psutil.virtual_memory()

print(f"Total memory: {memory.total / (1024**3):.2f} GB")
print(f"Available memory: {memory.available / (1024**3):.2f} GB")
print(f"Used memory: {memory.used / (1024**3):.2f} GB")
print(f"Memory usage: {memory.percent}%")

#%% First stage of IV
fs1_price = pf.feols("log_price ~ log_price_iv + krone_ends_with_nine + ore_ends_with_nine | sku_gtin + store_id + week", data=df, vcov = {'CRV1':'store_id'})
df["log_price_hat"] = fs1_price.predict()

df['log_price_krone_hat'] = df.log_price_hat * df.krone_ends_with_nine
df['log_price_ore_hat'] = df.log_price_hat * df.ore_ends_with_nine

#%% OLS IV
model_iv1 = pf.feols('log_quantity ~ 1 | sku_gtin + store_id + week | log_price ~ log_price_iv', data=df, vcov = {'CRV1':'store_id'})
model_iv1.summary()
del model_iv1
gc.collect()

model_iv2 = pf.feols('log_quantity ~ log_price_hat | sku_gtin + store_id + week', data=df, vcov = {'CRV1':'store_id'})
model_iv2.summary()
del model_iv2
gc.collect()

model_iv3 = pf.feols('log_quantity ~ log_price_hat + krone_ends_with_nine + log_price_krone_hat | sku_gtin + store_id + week', data=df, vcov = {'CRV1':'store_id'})
model_iv3.summary()
del model_iv3
gc.collect()

model_iv4 = pf.feols('log_quantity ~ log_price_hat + ore_ends_with_nine + log_price_ore_hat | sku_gtin + store_id + week', data=df, vcov = {'CRV1':'store_id'})
model_iv4.summary()
del model_iv4
gc.collect()

model_iv5 = pf.feols('log_quantity ~ log_price_hat + krone_ends_with_nine + log_price_krone_hat + ore_ends_with_nine + log_price_ore_hat  | sku_gtin + store_id + week', data=df, vcov = {'CRV1':'store_id'})
model_iv5.summary()
del model_iv5
gc.collect()

#%% Make a plot for predicted demand
price_grid = np.linspace(df["ppu"].min(), df["ppu"].max(), 100)
pred_df = pd.DataFrame({
    "price_nok": price_grid,
    "krone": price_grid.astype(int),
    "ore": ((price_grid - price_grid.astype(int)) * 100).round().astype(int)
})
pred_df["log_price_hat"] = np.log(pred_df["price_nok"])
pred_df["krone_ends_with_nine"] = (pred_df["krone"] % 10 == 9).astype(int)
pred_df["log_price_krone_hat"] = np.log(pred_df["krone"].replace(0, np.nan)).fillna(0)
pred_df["ore_ends_with_nine"] = (pred_df["ore"] % 10 == 9).astype(int)
pred_df["log_price_ore_hat"] = np.log(pred_df["ore"].replace(0, np.nan)).fillna(0)

pred_df["log_q_hat"] = model_iv5.predict(pred_df)

# Step 5: Plot
plt.figure(figsize=(8, 5))
plt.plot(pred_df["price_nok"], pred_df["log_q_hat"], label="Predicted log quantity", color="navy")
plt.xlabel("Price (NOK)")
plt.ylabel("Predicted log Quantity")
plt.title("Demand Curve (IV Prediction)")
plt.grid(True)
plt.tight_layout()
plt.show()

#%% MODELS for log quantity
model_q = pf.feols('log_quantity ~ log_price + sw(krone_ends_with_nine, ore_ends_with_nine) | sku_gtin + store_id + week', data=df, vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + sw(krone_ends_with_nine, ore_ends_with_nine) | sku_gtin + store_id + week', data=df[df.format=='discounter'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + sw(krone_ends_with_nine, ore_ends_with_nine) | sku_gtin + store_id + week', data=df[df.format=='convenience'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + sw(krone_ends_with_nine, ore_ends_with_nine) | sku_gtin + store_id + week', data=df[df.format=='supermarket'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

#%% MODELS for log quantity and interaction
model_q = pf.feols('log_quantity ~ log_price + log_price_krone + krone_ends_with_nine | sku_gtin + store_id + week', data=df, vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + ore_ends_with_nine+ log_price_ore | sku_gtin + store_id + week', data=df, vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()


model_q = pf.feols('log_quantity ~ log_price + krone_ends_with_nine + log_price_krone | sku_gtin + store_id + week', data=df[df.format=='discounter'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + ore_ends_with_nine+ log_price_ore | sku_gtin + store_id + week', data=df[df.format=='discounter'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + krone_ends_with_nine + log_price_krone | sku_gtin + store_id + week', data=df[df.format=='convenience'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + ore_ends_with_nine+ log_price_ore | sku_gtin + store_id + week', data=df[df.format=='convenience'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + krone_ends_with_nine + log_price_krone | sku_gtin + store_id + week', data=df[df.format=='supermarket'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

model_q = pf.feols('log_quantity ~ log_price + ore_ends_with_nine+ log_price_ore | sku_gtin + store_id + week', data=df[df.format=='supermarket'], vcov = {'CRV1':'store_id'})
model_q.summary()
del model_q
gc.collect()

#%% MODELS for log transactions
model_trans = pf.feols('log_trans ~ log_price + sw(krone_ends_with_nine, ore_ends_with_nine) | sku_gtin + store_id + week', data=df, vcov = {'CRV1':'store_id'})
model_trans.summary()
del model_trans
gc.collect()

model_trans = pf.feols('log_trans ~ log_price + sw(krone_ends_with_nine, ore_ends_with_nine) | sku_gtin + store_id + week', data=df[df.format=='discounter'], vcov = {'CRV1':'store_id'})
model_trans.summary()
del model_trans
gc.collect()

model_trans = pf.feols('log_trans ~ log_price + sw(krone_ends_with_nine, ore_ends_with_nine) | sku_gtin + store_id + week', data=df[df.format=='convenience'], vcov = {'CRV1':'store_id'})
model_trans.summary()
del model_trans
gc.collect()

model_trans = pf.feols('log_trans ~ log_price + sw(krone_ends_with_nine, ore_ends_with_nine) | sku_gtin + store_id + week', data=df[df.format=='supermarket'], vcov = {'CRV1':'store_id'})
model_trans.summary()
del model_trans
gc.collect()