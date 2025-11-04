'''
Explore Frode's ideas on LDP:
1. Check price dynamics for staple products
2. Check price dynamics for PL products
'''

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
import seaborn as sns
from matplotlib.lines import Line2D

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


#%% Function to check memory usage
def check_memory():
    memory = psutil.virtual_memory()
    print(f"Total memory: {memory.total / (1024**3):.2f} GB")
    print(f"Available memory: {memory.available / (1024**3):.2f} GB")
    print(f"Used memory: {memory.used / (1024**3):.2f} GB")
    print(f"Memory usage: {memory.percent}%")

# %% Data filtering and subsampling
df_subset = df_full[(df_full['price'] > 0) & (df_full['price'] < 100) & (df_full['quantity'] > 0) &  (df_full['quantity'] < 201)]
df_subset['sales'] = df_subset['price'] * df_subset['quantity']

#df = df_subset.sample(frac=0.1, random_state=42).copy()
del df_full
gc.collect()

#%%
def krone_ends_with_nine(price):
    """
    Checks if the integer part (kroner) of the price ends with 9, 
    but not if the integer part is >= 90.
    """
    if pd.isna(price):
        return False
    kroner_part = int(price)
    return kroner_part % 10 == 9 and kroner_part < 90

def ore_ends_with_nine(price):
    """
    Checks if the decimal part (øre) is exactly 0.90, 0.95, or 0.99.
    Also accepts 0.9 as 0.90.
    """
    if pd.isna(price):
        return False
    ore = round(price * 100) % 100  # Extract øre as an integer (e.g. 12.95 -> 95)
    return ore in [90, 95, 99]

# Plot price dynamics for Freia Melkesjokolade 100g
def plot_price_dynamics_with_endings(df, product_name):
    plt.figure(figsize=(12, 6))
    ax = sns.lineplot(
    data=df, x='week', y='ppu', hue='kjedeid',
    palette='tab10', errorbar=None)
    # Nice endings (● with black edge)
    sns.scatterplot(
        data=df[df['krone_ends_with_nine']],
    x='week', y='ppu', hue='kjedeid',
    marker='o', s=80, edgecolor='black', linewidth=0.8, legend=False
    )

    # X markers for ore-ending-with-nine
    sns.scatterplot(
        data=df[df['ore_ends_with_nine']],
        x='week', y='ppu', hue='kjedeid',
        marker='x', s=100, edgecolor='black', linewidth=2, legend=False
    )

    # Get kjedeid legend handles from seaborn
    handles, labels = ax.get_legend_handles_labels()

    # Separate kjedeid handles (ignore automatic entries for hues)
    kjedeid_handles = handles  # skip the "hue" title entry
    kjedeid_labels = labels

    # Custom handles for marker types
    custom_lines = [
        Line2D([0], [0], marker='o', color='w', markerfacecolor='gray',
            markeredgecolor='black', markersize=8, label='Krone ends with 9'),
        Line2D([0], [0], marker='x', color='black', markersize=8,
            label='Øre ends with 9')
    ]

    # Combine both
    first_legend = plt.legend(kjedeid_handles, kjedeid_labels, title='Chain', loc='lower right')
    plt.gca().add_artist(first_legend)
    plt.legend(handles=custom_lines, title='Price ending type', loc='upper right')

    plt.xlabel('Week')
    plt.ylabel('Price')
    plt.title(f'Price Dynamics for {product_name}')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# %%
df_freia = df_subset.loc[(df_subset['sku_gtin'] == 7040110569908) & (df_subset['kjedeid'].isin(['kiwi', 'Extra', 'Rema'])), :].groupby(['week', 'kjedeid']).agg(lambda x: x.value_counts().idxmax()).reset_index()

#  Calculate LDP
df_freia.loc[:, 'krone_ends_with_nine'] = df_freia['ppu'].apply(krone_ends_with_nine)
df_freia.loc[:, 'ore_ends_with_nine'] = df_freia['ppu'].apply(ore_ends_with_nine)
df_freia.loc[:, 'ends_with_nine'] = df_freia['krone_ends_with_nine'] | df_freia['ore_ends_with_nine']

plot_price_dynamics_with_endings(df_freia, 'Freia Melkesjokolade 100g')
# %%
df_tine_melk = df_subset.loc[(df_subset['sku_gtin'] == 7038010056765) & (df_subset['kjedeid'].isin(['kiwi', 'Extra', 'Rema'])), :].groupby(['week', 'kjedeid']).agg(lambda x: x.value_counts().idxmax()).reset_index()

#  Calculate LDP
df_tine_melk.loc[:, 'krone_ends_with_nine'] = df_tine_melk['ppu'].apply(krone_ends_with_nine)
df_tine_melk.loc[:, 'ore_ends_with_nine'] = df_tine_melk['ppu'].apply(ore_ends_with_nine)
df_tine_melk.loc[:, 'ends_with_nine'] = df_tine_melk['krone_ends_with_nine'] | df_tine_melk['ore_ends_with_nine']

# Plot price dynamics for Tine Melk
plot_price_dynamics_with_endings(df_tine_melk, 'Tine Melk 1L')
# %% OST
df_ost = df_subset.loc[(df_subset['sku_gtin'] == 7038010023279) & (df_subset['kjedeid'].isin(['kiwi', 'Extra', 'Rema'])), :].groupby(['week', 'kjedeid']).agg(lambda x: x.value_counts().idxmax()).reset_index()

#  Calculate LDP
df_ost.loc[:, 'krone_ends_with_nine'] = df_ost['ppu'].apply(krone_ends_with_nine)
df_ost.loc[:, 'ore_ends_with_nine'] = df_ost['ppu'].apply(ore_ends_with_nine)
df_ost.loc[:, 'ends_with_nine'] = df_ost['krone_ends_with_nine'] | df_ost['ore_ends_with_nine']

plot_price_dynamics_with_endings(df_ost, 'Norvegia Ost 400g')
# %% PRIVATE BRANDS: PASTA
df_pasta_pl = df_subset.loc[(df_subset['sku_gtin'] == 7035620018930) & (df_subset['kjedeid'].isin(['kiwi', 'joker', 'meny', 'spar'])), :].groupby(['week', 'kjedeid']).agg(lambda x: x.value_counts().idxmax()).reset_index()

#  Calculate LDP
df_pasta_pl.loc[:, 'krone_ends_with_nine'] = df_pasta_pl['ppu'].apply(krone_ends_with_nine)
df_pasta_pl.loc[:, 'ore_ends_with_nine'] = df_pasta_pl['ppu'].apply(ore_ends_with_nine)
df_pasta_pl.loc[:, 'ends_with_nine'] = df_pasta_pl['krone_ends_with_nine'] | df_pasta_pl['ore_ends_with_nine']

plot_price_dynamics_with_endings(df_pasta_pl, 'Pasta Eldorado')
# %% PRIVATE BRANDS: OLIVE OIL
df_olive_oil_pl = df_subset.loc[(df_subset['sku_gtin'] == 6410708762683) & (df_subset['kjedeid'].isin(['kiwi', 'joker', 'meny', 'spar'])), :].groupby(['week', 'kjedeid']).agg(lambda x: x.value_counts().idxmax()).reset_index()

#  Calculate LDP
df_olive_oil_pl.loc[:, 'krone_ends_with_nine'] = df_olive_oil_pl['ppu'].apply(krone_ends_with_nine)
df_olive_oil_pl.loc[:, 'ore_ends_with_nine'] = df_olive_oil_pl['ppu'].apply(ore_ends_with_nine)
df_olive_oil_pl.loc[:, 'ends_with_nine'] = df_olive_oil_pl['krone_ends_with_nine'] | df_olive_oil_pl['ore_ends_with_nine']

plot_price_dynamics_with_endings(df_olive_oil_pl, 'Olive Oil')

#%% Check popular vs. non-popular products
df_subset['sales'] = df_subset['price'] * df_subset['quantity']
num_trans_prd = df_subset.groupby('sku_gtin', as_index=False)['trans'].sum()

plt.figure(figsize=(12, 6))
sns.barplot(x=num_trans_prd['sku_gtin'], y=num_trans_prd['trans'])
plt.title('Number of transactions per product')
plt.xlabel('Product ID')
plt.ylabel('Number of transactions')
plt.show()
# %% Explore LDP among popular and unpopular products
df_subset['krone_ends_with_nine'] = df_subset['ppu'].apply(krone_ends_with_nine)
df_subset['ore_ends_with_nine'] = df_subset['ppu'].apply(ore_ends_with_nine)
df_subset['ends_with_nine'] = df_subset['krone_ends_with_nine'] | df_subset['ore_ends_with_nine']
ldp_prd_chain = df_subset.groupby(['sku_gtin', 'kjedeid'], as_index=False)[['krone_ends_with_nine', 'ore_ends_with_nine']].mean()


df_subset['sales'] = df_subset['price']*df_subset['quantity']
product_popularity = (
    df_subset.groupby('sku_gtin')[['trans', 'quantity', 'sales']].sum().reset_index()
    .rename(columns={'trans': 'total_transactions', 'quantity': 'total_quantity', 'sales': 'total_sales'})
)

ldp_prd_chain = ldp_prd_chain.merge(product_popularity, on='sku_gtin', how='left')

# --- Sort products by popularity ---
ldp_prd_chain = ldp_prd_chain.sort_values('total_transactions', ascending=False)

# --- Plot ---
plt.figure(figsize=(12, 6))
sns.barplot(
    data=ldp_prd_chain[ldp_prd_chain.kjedeid == 'kiwi'],
    x='sku_gtin', y='krone_ends_with_nine',
    order=ldp_prd_chain['sku_gtin'],
    color='skyblue'
)
plt.xticks(rotation=45, ha='right')
plt.xlabel('Product')
plt.ylabel('Share of LDP Prices')
plt.title('Left-Digit Pricing Frequency by Product Popularity')
plt.tight_layout()
plt.show()

#%% 
df_chain_prd_week = df_subset.groupby(['kjedeid','sku_gtin', 'week']).agg({'ppu': lambda x: x.value_counts().idxmax(), 'quantity': 'sum', 'trans': 'sum', 'sales': 'sum'}).reset_index()


df_chain_prd_week['min_price_week'] = (
    df_chain_prd_week.groupby(['sku_gtin', 'week'])['ppu'].transform('min')
)
df_chain_prd_week['min_price_week_lag'] = (
    df_chain_prd_week.groupby('sku_gtin')['min_price_week'].shift(1)
)

df_chain_prd_week['price_at_min_lag'] = df_chain_prd_week['ppu'] == df_chain_prd_week['min_price_week_lag']

df_chain_prd_week['price_lower_lag'] = df_chain_prd_week['ppu'] < df_chain_prd_week['min_price_week_lag']

df_chain_prd_week['price_higher_lag'] = df_chain_prd_week['ppu'] > df_chain_prd_week['min_price_week_lag']

df_chain_prd_week['min_price_lag_ldp'] = df_chain_prd_week['min_price_week_lag'].apply(krone_ends_with_nine) 

df_chain_prd_week['min_price_lag_int'] = df_chain_prd_week['min_price_week_lag'] % 10 == 0

df_chain_prd_week['ppu_ldp'] = df_chain_prd_week['ppu'].apply(krone_ends_with_nine)

#%% Analyze how often chains match or beat the lowest price from the previous week

mod1 = pf.feols("ppu_ldp ~ price_at_min_lag + price_lower_lag  | sku_gtin + week", data=df_chain_prd_week)

mod2 = pf.feols("ppu_ldp ~ price_at_min_lag + price_lower_lag  + price_at_min_lag*min_price_lag_int + price_lower_lag*min_price_lag_int | sku_gtin + week", data=df_chain_prd_week)
print(mod2.summary())

mod3 = pf.feols("ppu_ldp ~ price_at_min_lag + price_lower_lag  + price_at_min_lag*min_price_lag_int + price_lower_lag*min_price_lag_int + price_at_min_lag*min_price_lag_ldp + price_lower_lag*min_price_lag_ldp | sku_gtin + week", data=df_chain_prd_week)
print(mod3.summary())
