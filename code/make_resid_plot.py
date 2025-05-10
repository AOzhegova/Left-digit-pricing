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
from duckreg.estimators import DuckRegression
import seaborn as sns
from sklearn.linear_model import LinearRegression
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
df_subset = df_full[(df_full['price'] > 0) & (df_full['ppu'] < 100) & (df_full['quantity'] > 0) & (df_full['quantity'] < 100)].copy()


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

# %% Define functions to check if price ends with 9
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

df.loc[:, 'krone_ends_with_nine'] = df['ppu'].apply(krone_ends_with_nine)
df.loc[:, 'ore_ends_with_nine'] = df['ppu'].apply(ore_ends_with_nine)
df.loc[:, 'ends_with_nine'] = df['krone_ends_with_nine'] | df['ore_ends_with_nine']

#%% Add necessary columns for regression
df['log_price'] = np.log(df['ppu'])
df['log_quantity'] = np.log(df['quantity'])
df['log_trans'] = np.log(df['trans'])
df['log_price_krone'] = df.log_price * df.krone_ends_with_nine
df['log_price_ore'] = df.log_price * df.ore_ends_with_nine

#%% Add product characteristics
# Load data with product characteristics 
kassalapp = pd.read_feather(r"M:\grocery_server\data\kassalapp_data_food.ftr")
kassalapp['category_name'] = kassalapp['category_name'].astype('category')
kassalapp['parent_category_name'] = kassalapp['parent_category_name'].astype('category')
kassalapp_short = kassalapp.loc[:,['ean','category_name','parent_category_name', 'food_cat']].drop_duplicates()
df['sku_gtin'] = df['sku_gtin'].astype('category')
df['kassalapp'] = df['sku_gtin'].isin(kassalapp['ean'])
kassalapp_short['ean'] = kassalapp_short['ean'].astype('category')

df = df.merge(kassalapp_short[['ean', 'parent_category_name', 'category_name', 'food_cat']], left_on='sku_gtin', right_on='ean', how='left')

#%% Focus on one category
df_cat = df[(df['parent_category_name'] == 'Kaffe')].copy()
#df_cat = df
df_cat['ppu'] = df_cat['ppu'].round(3)
df_cat['floor'] = np.floor(df_cat['ppu'])
df_cat = df_cat[(df_cat['floor']<60) & (df_cat['floor']>0)]
df_cat['group'] = (df_cat['floor'] // 10) * 10 
df_cat['avg_price'] = df_cat.groupby(['week', 'store_id'])['ppu'].transform('mean')
# df_cat['sku_kjede'] = df_cat['sku_gtin'].astype(str) + '_' + df_cat['kjedeid'].astype(str)


#%% Run FEOLS
model_q = pf.feols('log_quantity ~ i(floor) | sku_gtin + week + store_id', data=df_cat)
print(model_q.summary())

#%% Extract the coefficients and standard errors for the krone_bin dummies
coeffs = model_q.tidy().reset_index()
krone_bin_coefs = coeffs[coeffs['Coefficient'].str.startswith('C(floor)')].copy()
# Extract the numeric part of the floor_bin from the Coefficient column
krone_bin_coefs['floor_bin'] = krone_bin_coefs['Coefficient'].str.extract(r'C\(floor\)\[T\.(\d+\.\d+)\]').astype(float)
krone_bin_coefs['group'] = (krone_bin_coefs['floor_bin'] // 10) * 10

#%% SCATTER PLOT
# Plot coefficients
plt.figure(figsize=(10, 6))
sns.scatterplot(data=krone_bin_coefs, x='floor_bin', y='Estimate', hue='group', palette='tab10')
plt.xlabel('Price Bin (Floor)')
plt.ylabel('Coefficient Estimate')
plt.title('Coefficient Estimates by Price Bin')
plt.grid(True)
plt.tight_layout()
plt.show()


#%% Build a plot
# Sort and clean
krone_bin_coefs.sort_values('floor_bin', inplace=True)

# Plot error bars (as before)
plt.figure(figsize=(10, 6))
plt.errorbar(
    x=krone_bin_coefs['floor_bin'],
    y=krone_bin_coefs['Estimate'],
    yerr=[
        krone_bin_coefs['Estimate'] - krone_bin_coefs['2.5%'],
        krone_bin_coefs['97.5%'] - krone_bin_coefs['Estimate']
    ],
    fmt='o', color='black', ecolor='gray', capsize=4, label='Estimate ± 95% CI', alpha=0.5
)

# Define bin ranges for local linear fits
bin_edges = np.arange(0, krone_bin_coefs['floor_bin'].max() + 10, 10)

# Loop through ranges and fit local linear trend
for i in range(len(bin_edges) - 1):
    bin_start = bin_edges[i]
    bin_end = bin_edges[i + 1]
    
    # Filter subset of data
    subset = krone_bin_coefs[(krone_bin_coefs['floor_bin'] >= bin_start) & (krone_bin_coefs['floor_bin'] < bin_end)]
    
    if len(subset) >= 2:
        X = subset[['floor_bin']]
        y = subset['Estimate']
        reg = LinearRegression().fit(X, y)
        x_range = np.linspace(bin_start, bin_end, 100).reshape(-1, 1)
        y_pred = reg.predict(x_range)
        plt.plot(x_range, y_pred, alpha=0.99, linewidth=3)

# Final touches
plt.axhline(0, color='gray', linestyle='--')
plt.xlabel('Price, NOK')
plt.ylabel('Effect on log quantity')
plt.title('Demand curve')
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

# %%
