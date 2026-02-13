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
import warnings
warnings.filterwarnings('ignore')

pd.options.display.float_format = '{:.2f}'.format  
pd.set_option('display.max_columns', None)  # Show all columns in DataFrame output

current_dir = Path(__file__)
gitfolder = current_dir.parent.parent
datafolder = current_dir.parent.parent.parent / 'data'
output = gitfolder / 'output' 
codes_path = output / "selected_product_codes.parquet"
#codes_path = datafolder / "common_skus.csv"

#%%
kassalapp = pd.read_feather(f'{datafolder}\\kassalapp_data_food.ftr')
kassalapp.parent_category_name = kassalapp.parent_category_name.astype('category')
cat_product_codes = kassalapp[kassalapp['parent_category_name'] == 'Sauser'].ean.unique()
cat_product_codes = pd.DataFrame(cat_product_codes, columns=['ean'])
#cat_product_codes.to_parquet(codes_path, index=False)

del kassalapp
gc.collect()


#%% Load data
# Connect to DuckDB
con = duckdb.connect()

# Define file paths
base_path = datafolder / "Aggregate Data"
#months = ["june", "july", "aug", "sep", "oct", "nov"]
months = ["june"]

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
    
    data_query = f"""
        SELECT d.*
        FROM read_parquet('{base_path / (month + '_data_week_store_sku.parquet')}') AS d
        SEMI JOIN read_parquet('{codes_path}') AS c
        ON d.sku_gtin = c.ean
    """
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

#%% Function to check memory usage
def check_memory():
    memory = psutil.virtual_memory()
    print(f"Total memory: {memory.total / (1024**3):.2f} GB")
    print(f"Available memory: {memory.available / (1024**3):.2f} GB")
    print(f"Used memory: {memory.used / (1024**3):.2f} GB")
    print(f"Memory usage: {memory.percent}%")

# %%
df_subset = df_full[(df_full['ppu'] >= 1) & (df_full['ppu'] < 100) & (df_full['quantity'] > 0) &  (df_full['quantity'] < 201)].copy()

del df_full
gc.collect()

# %%
product_unique = df_subset.groupby('sku_gtin', as_index=False).agg({'store_id': 'nunique', 'week': 'nunique'})

product_nonunique = product_unique[(product_unique['store_id'] > 1) & (product_unique['week'] > 1)].sku_gtin.unique()

df_nonunique = df_subset[df_subset['sku_gtin'].isin(product_nonunique)].copy()

del df_subset
gc.collect()

check_memory()

#%%
def krone_distance(price):
    """
    price: float or pd.Series (e.g. 49.90)

    returns:
    - dK: distance to lower decade in kroner (49.90 -> 9)
    - dO: distance to 0 ore (49.90 -> 0.90)
    """

    price = np.asarray(price)

    # Krone and ore decomposition
    krone_part = np.floor(price).astype(int)
    ore_part = price - krone_part

    # Distance to lower 10-krone threshold
    dK = 1 - (krone_part/10 - np.floor(krone_part / 10))
    # Distance to 0 ore
    dO = ore_part

    return dK

def ore_distance(price):
    """
    price: float or pd.Series (e.g. 49.90)

    returns:
    - dO: distance to 0 ore (49.90 -> 0.90)
    """

    price = np.asarray(price)

    # Krone and ore decomposition
    krone_part = np.floor(price).astype(int)
    ore_part = price - krone_part

    # Distance to 0 ore
    dO = 1 - ore_part

    return dO

def take_krone_part(price):
    price = np.asarray(price)
    krone_part = np.floor(price).astype(int)
    return krone_part

def take_ore_part(price):
    price = np.asarray(price)
    krone_part = np.floor(price).astype(int)
    ore_part = price - krone_part
    return ore_part

def take_decade(price):
    price = np.asarray(price)
    krone_part = np.floor(price).astype(int)
    return krone_part // 10 * 10

#%% Add necessary columns for regression
df_nonunique['log_price'] = np.log(df_nonunique['ppu'])
df_nonunique['log_quantity'] = np.log(df_nonunique['quantity'])
df_nonunique['log_trans'] = np.log(df_nonunique['trans'])
df_nonunique["dK"] = krone_distance(df_nonunique["ppu"])
df_nonunique["dO"] = ore_distance(df_nonunique["ppu"])
df_nonunique["krone_part"] = take_krone_part(df_nonunique["ppu"])
df_nonunique["ore_part"] = take_ore_part(df_nonunique["ppu"])
df_nonunique['log_krone_part'] = np.log(df_nonunique["krone_part"])
df_nonunique['log_ore_part'] = np.log(df_nonunique["ore_part"] + 0.01)
df_nonunique['decade'] = take_decade(df_nonunique["ppu"])
#%%
model1 = pf.feols('log_quantity ~ log_price | sku_gtin + store_id + week', data=df_nonunique, vcov = {'CRV1':'store_id'})
model1.summary()
del model1   
gc.collect()

# %%
model4 = pf.feols('log_quantity ~ ppu + krone_part + decade | sku_gtin + kjedeid + week', data=df_nonunique, vcov = {'CRV1':'store_id'})
model4.summary()

beta_price = model4.coef()[0]
beta_ore = model4.coef()[1]
beta_krone = model4.coef()[2]
theta_ore = beta_ore/(beta_price+beta_ore+beta_krone)
print(f"Ore bias: {theta_ore:.4f}")
theta_krone = beta_krone/(beta_price+beta_ore+beta_krone)
print(f"Krone bias: {theta_krone:.4f}")


# %%
results = []

for (sku_gtin, chain), group in df_nonunique.groupby(["sku_gtin", "kjedeid"]):

    print(f"Processing {(sku_gtin, chain)}...")

    group = group[
        ["log_quantity", "ppu", "krone_part", "decade"]
    ].dropna()

    # Minimum observations
    if len(group) < 10:
        continue

    # Require price variation
    if group["ppu"].nunique() < 2:
        continue

    # Check matrix rank BEFORE running regression
    X = group[["ppu", "krone_part", "decade"]].to_numpy()
    rank = np.linalg.matrix_rank(X)

    # If rank < number of regressors, skip this cell
    if rank < X.shape[1]:
        continue

    try:
        demand_model = pf.feols(
            'log_quantity ~ ppu + krone_part + decade',
            data=group
        )

        coefs = demand_model.coef()

        beta_price = coefs.get("ppu", np.nan)
        beta_krone = coefs.get("krone_part", np.nan)
        beta_decade = coefs.get("decade", np.nan)

        # Only compute theta if all three exist
        if not any(np.isnan([beta_price, beta_krone, beta_decade])):
            denom = beta_price + beta_krone + beta_decade
            if denom != 0:
                theta_krone = beta_krone / denom
                theta_decade = beta_decade / denom
            else:
                theta_krone = np.nan
                theta_decade = np.nan
        else:
            theta_krone = np.nan
            theta_decade = np.nan

        results.append({
            "sku_gtin": sku_gtin,
            "kjedeid": chain,
            "n_obs": len(group),
            "theta_krone": theta_krone,
            "theta_decade": theta_decade,
            "beta_price": beta_price
        })

        del demand_model
        gc.collect()

    except Exception:
        # Skip singular or failed regressions
        continue

results_df = pd.DataFrame(results)
# %%
