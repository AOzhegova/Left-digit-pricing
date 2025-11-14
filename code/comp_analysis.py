'''
Here I run competition analysis of left-digit pricing
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
from statsmodels.iolib.summary2 import summary_col
from tabulate import tabulate

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
#months = ["june"]
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

#df = df_subset.sample(frac=0.1, random_state=42).copy()
del df_full
gc.collect()

#%% Define function to calculate LDP
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

# %% Calculate LDP
df_subset.loc[:, 'krone_ends_with_nine'] = df_subset['ppu'].apply(krone_ends_with_nine)
df_subset.loc[:, 'ore_ends_with_nine'] = df_subset['ppu'].apply(ore_ends_with_nine)
df_subset.loc[:, 'ends_with_nine'] = df_subset['krone_ends_with_nine'] | df_subset['ore_ends_with_nine']


#%% Aggregate to store-sku-week level
df_subset['sales'] = df_subset['price'] * df_subset['quantity']
# Calculate percentage of 9-endings in every store
store_df = df_subset.groupby(['store_id'], as_index=False).agg({'ends_with_nine': 'mean', 'krone_ends_with_nine': 'mean', 'ore_ends_with_nine': 'mean', 'kjedeid': 'first', 'trans': 'sum', 'sales': 'sum'})

del df_subset
gc.collect()

store_df.store_id = store_df.store_id.astype(float).astype('int64')
store_df.kjedeid = store_df.kjedeid.astype('category')

#%% # Load geodata, keys: store id
stores = gpd.read_file(r'M:\grocery_server\data\geodata\NARING_Dagligvare.gdb')
stores['id_int'] = (
    stores['id'].astype(str)
    .str.replace(r'[^0-9]', '', regex=True)
    .pipe(pd.to_numeric, errors='coerce')
    .astype('Int64')
)
stores['gln_clean'] = (
    stores['gln'].astype(str)
    .str.replace(r'[^0-9]', '', regex=True)
)

geodata_id  = stores[['id_int', 'paraplykjede', 'geometry']].dropna(subset=['id_int']).drop_duplicates('id_int')
geodata_gln = stores[['gln_clean','paraplykjede', 'geometry']].dropna(subset=['gln_clean']).drop_duplicates('gln_clean')

# --- Normalize store_df keys (no slices) ---
store_df = store_df.copy()
store_df['store_id_int'] = pd.to_numeric(store_df['store_id'], errors='coerce').astype('Int64')
store_df['store_id_str'] = store_df['store_id_int'].astype(str)

# --- Merge on id_int and gln_clean separately (full frames, same row order) ---
m_id  = store_df.merge(geodata_id,  left_on='store_id_int', right_on='id_int',   how='left', suffixes=('', '_id'))
m_gln = store_df.merge(geodata_gln, left_on='store_id_str', right_on='gln_clean', how='left', suffixes=('', '_gln'))

# Ensure indices match for safe row-wise coalescing
m_id.index = store_df.index
m_gln.index = store_df.index

# --- Coalesce columns: prefer id_int match, else gln match ---
out = store_df.copy()
out['geometry']     = m_id['geometry'].combine_first(m_gln['geometry'])
out['kjedeid']      = m_id['kjedeid'].combine_first(m_gln['kjedeid'])
out['paraplykjede'] = m_id['paraplykjede'].combine_first(m_gln['paraplykjede'])

# Optional: track which key matched
out['matched_on'] = np.where(m_id['geometry'].notna(), 'id_int',
                      np.where(m_gln['geometry'].notna(), 'gln_clean', pd.NA))

# Return as GeoDataFrame (keep CRS from geodata)
df = gpd.GeoDataFrame(out, geometry='geometry', crs=stores.crs)

del stores
gc.collect()

#%%
# Variance of prices with 9-endings across stores
df[['ends_with_nine', 'krone_ends_with_nine', 'ore_ends_with_nine']].describe()

#%% Plot a hist of 9-endings frequences
plt.hist(df.krone_ends_with_nine, bins=60, color='grey', alpha=0.8, edgecolor='black', label='At krone-level')
plt.hist(df.ore_ends_with_nine, bins=60, color='red', alpha=1.0, edgecolor='black', label='At øre-level')

average_krone = df.krone_ends_with_nine.mean()
average_ore = df.ore_ends_with_nine.mean()

# Adding labels and title for clarity
plt.xlabel('Stores')
plt.ylabel('Frequency of Left-Digit Pricing at Store Level')
#plt.title('Distribution of Stores with Left-digit Pricing')
plt.legend(loc='upper right')

# Display the plot
plt.show()

# %%
# Calculate mean and variance of 9-endings in prices by chains
print(store_df.groupby('kjedeid')[['krone_ends_with_nine', 'ore_ends_with_nine']].agg(['mean', 'std']))

#%% Assign chains to formats
chain_format_mapping = {'Extra': 'discounter', 'Prix': 'discounter', 'kiwi': 'discounter', 'Rema': 'discounter', 'meny': 'supermarket', 'spar': 'supermarket', 'Mega': 'supermarket', 'Obs': 'hypermarket', 'joker': 'convenience', 'Marked': 'convenience', 'Matkroken': 'convenience', 'nærbutikken': 'convenience'}

store_df['format'] = store_df.kjedeid.map(chain_format_mapping)

# %%
kjede_ids = ['Extra', 'Prix', 'Marked', 'Matkroken', 'Mega', 'Obs']
colors = ['#073b4c', '#06d6a0', '#ffd166', '#118ab2', '#f4a261', '#e76f51']

# Create subplots
fig, axs = plt.subplots(3, 2, figsize=(15, 18))
axs = axs.flatten()

# Plot histograms for each kjede
for ax, kjede_id, color in zip(axs, kjede_ids, colors):
    subset = store_df[store_df.kjedeid == kjede_id]
    
    # Plot krone_ends_with_nine
    ax.hist(subset.krone_ends_with_nine, bins=30, color=color, edgecolor='black', alpha=0.9, label='At krone-level')
    
    # Plot ore_ends_with_nine
    ax.hist(subset.ore_ends_with_nine, bins=30, color=color, edgecolor='black', alpha=0.7, hatch='x', label='At øre-lvel')
    
    ax.set_xlabel('Frequence of Left-digit Pricing')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Distribution of Left-digit Pricing for {kjede_id}')
    ax.legend()

# Adjust layout
plt.tight_layout()
plt.show()
# %%
# Correlation between frequence of ore_ends_with_nine and demand
print("Correlation between ore_ends_with_nine and the number of transactions: {:.3f}".format(store_df.ore_ends_with_nine.corr(store_df.trans)))
print("Correlation between ore_ends_with_nine and revenue: {:.3f}".format(store_df.ore_ends_with_nine.corr(store_df.sales)))
print("======")
print("Correlation between krone_ends_with_nine and the number of transactions: {:.3f}".format(store_df.krone_ends_with_nine.corr(store_df.trans)))
print("Correlation between krone_ends_with_nine and revenue: {:.3f}".format(store_df.krone_ends_with_nine.corr(store_df.sales)))

# %% Functions to calculate the minimum distance to stores
def min_distance_to_other_category(gdf, column):
    min_distances = []
    for idx, row in gdf.iterrows():
        other_stores = gdf[gdf[column] != row[column]]
        distances = other_stores.distance(row['geometry'])
        min_distances.append(distances.min())
    return min_distances

def count_competitors_within_distance(gdf, distances_km=[2, 5, 10]):
    'Calculate number of competitors within specified distances'
    # Convert distances from km to meters
    distances_m = [dist * 1000 for dist in distances_km]

    # Initialize columns for competitor counts
    for dist_km in distances_km:
        gdf[f'competitors_within_{dist_km}km'] = 0

    # Iterate over each store to calculate competitors within the specified distances
    for idx, row in gdf.iterrows():
        other_stores = gdf[gdf['kjedeid'] != row['kjedeid']]
        distances = other_stores.distance(row['geometry'])

        for dist_km, dist_m in zip(distances_km, distances_m):
            count = (distances <= dist_m).sum()
            gdf.at[idx, f'competitors_within_{dist_km}km'] = count
            
    return gdf

# %%  Calculate the minimum distance to closest store, closest store of different chain, and closest store of different retail group 
df = df[df.geometry.notna()].copy()
df['min_distance_all'] = df['geometry'].apply(lambda geom: df.distance(geom).replace(0, np.nan).min())
df['min_distance_diff_kjedeid'] = min_distance_to_other_category(df, 'kjedeid')
df['min_distance_diff_paraplykjede'] = min_distance_to_other_category(df, 'paraplykjede')

#%% Calculate local monopoly indicators
df['local_monopoly_2km'] = df['min_distance_diff_kjedeid'] > 2000
df['local_monopoly_5km'] = df['min_distance_diff_kjedeid'] > 5000
df['local_monopoly_10km'] = df['min_distance_diff_kjedeid'] > 10000

#%% Calculate number of competitors within 2km, 5km, and 10km
df = count_competitors_within_distance(df, distances_km=[2, 5, 10])


#%% Run regressions for ore_ends_with_nine and krone_ends_with_nine
m_2km_ore = smf.ols('ore_ends_with_nine ~ local_monopoly_2km + C(kjedeid)', data=df).fit()
print(m_2km_ore.summary())

m_5km_ore = smf.ols('ore_ends_with_nine ~ local_monopoly_5km + C(kjedeid)', data=df).fit()
print(m_5km_ore.summary())

m_10km_ore = smf.ols('ore_ends_with_nine ~ local_monopoly_10km + C(kjedeid)', data=df).fit()
print(m_10km_ore.summary())

m_2km_krone = smf.ols('krone_ends_with_nine ~ local_monopoly_2km + C(kjedeid)', data=df).fit()
print(m_2km_krone.summary())

m_5km_krone = smf.ols('krone_ends_with_nine ~ local_monopoly_5km + C(kjedeid)', data=df).fit()
print(m_5km_krone.summary())

m_10km_krone = smf.ols('krone_ends_with_nine ~ local_monopoly_10km + C(kjedeid)', data=df).fit()
print(m_10km_krone.summary())

# summarize all models (statsmodels OLS results)
models = [m_2km_ore, m_5km_ore, m_10km_ore, m_2km_krone, m_5km_krone, m_10km_krone]
model_names = ['2km_ore', '5km_ore', '10km_ore', '2km_krone', '5km_krone', '10km_krone']
regressors = ['local_monopoly_2km', 'local_monopoly_5km', 'local_monopoly_10km','Intercept', 'C(kjedeid)[T.Marked]', 'C(kjedeid)[T.Matkroken]',
'C(kjedeid)[T.Mega]', 'C(kjedeid)[T.Obs]', 'C(kjedeid)[T.Prix]',
'C(kjedeid)[T.Rema]', 'C(kjedeid)[T.joker]', 'C(kjedeid)[T.kiwi]',
'C(kjedeid)[T.meny]', 'C(kjedeid)[T.nærbutikken]', 'C(kjedeid)[T.spar]']

results_table = summary_col(
     [m_2km_ore, m_5km_ore, m_10km_ore, m_2km_krone, m_5km_krone, m_10km_krone],
    stars=True,
    float_format='%0.3f',
    model_names=model_names,
    info_dict={'N': lambda x: f"{int(x.nobs)}"},
    regressor_order=regressors)
print(results_table)

result_df = results_table.tables[0] if hasattr(results_table.tables[0], "columns") else None
md = tabulate(result_df, headers="keys", tablefmt="github", showindex=True)
print(md)

# %% Explore discounts
modes = df_subset.groupby(['gtin','date','kjede'])['ppu'] \
    .agg(lambda x: x.value_counts().idxmax()).reset_index(name='black_price')
df_subset = df_subset.merge(modes, on=['gtin','date','kjede'], how='left')
df_subset['is_discount'] = df_subset['ppu'] < df_subset['black_price']