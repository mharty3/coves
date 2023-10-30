import re
from pathlib import Path

from bs4 import BeautifulSoup
import duckdb
import geopandas as gpd
import pandas as pd
import requests
from tqdm import tqdm


Path("./data/roads").mkdir(parents=True, exist_ok=True)
Path("./data/counties").mkdir(parents=True, exist_ok=True)
Path("./data/states").mkdir(parents=True, exist_ok=True)

# create duckdb database
con = duckdb.connect('data/roads.db')

con.install_extension("httpfs")
con.load_extension("httpfs")

con.install_extension("spatial")
con.load_extension("spatial")

# get the links to the zipped TIGER shapefiles from the census website
pfix = 'https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/'
r = requests.get(pfix)
soup = BeautifulSoup(r.content, 'html.parser')
links = soup.find_all("a") # Find all elements with the tag <a>

links = [pfix + link.get("href") for link in links[6] 
             if link.get('href').startswith('tl_rd22')]

# use geopandas to read the zipped shapefiles 
# and then save them as parquet files

# it doesn't seem like duckdb can read zipped shapefiles
pattern = r'_(\d{5})_'
for link in tqdm(links):
    # regex to extract the county fips code from the link to add to the dataframe
    matches = re.findall(pattern, link) 
    (gpd.read_file(link)
        .assign(county_fips=matches[0])
        .to_parquet(f'data/roads/{matches[0]}.parquet')
    )

# create a table in the database containing all the roads from the shapefiles
con.sql(
    """
    CREATE TABLE IF NOT EXISTS roads AS
    SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry)
    AS geometry FROM 'data/roads/*.parquet'
    """
)


# do similar for the counties and states shapefiles
counties = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/COUNTY/tl_rd22_us_county.zip')
counties.to_parquet('data/counties/counties.parquet')

con.sql(
    """
    CREATE TABLE IF NOT EXISTS counties AS
    SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS geometry 
    FROM 'data/counties/counties.parquet'
    """
)

states = gpd.read_file('https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/STATE/tl_rd22_us_state.zip')
states.to_parquet('data/states/states.parquet')

con.sql(
    """
    CREATE TABLE IF NOT EXISTS counties AS
    SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry) AS geometry 
    FROM 'data/states/states.parquet'
    """
)

# Get a lookup table for the county codes and related data 
# I had to use pandas because duckdb doesn't support the encoding of the csv
co_fips = pd.read_csv('https://raw.githubusercontent.com/kjhealy/fips-codes/master/county_fips_master.csv', encoding='ISO-8859-1')
con.sql("""CREATE TABLE IF NOT EXISTS county_fips_lookup AS
            SELECT * from co_fips""")



# DuckDB query to get the number of coves in each county and save the result as a GeoJSON file
# DuckDB has nice regex support

# Pattern to remove the direction from the name if it occurs at the end
direction_pattern = r' (N|S|E|W|NE|SE|SW|NW)$' 

# Pattern to get the suffix from the name. Basically the last word
suffix_pattern = r'\b(\w+)\b$'

cove_count_q = f""" 
with 
    suffixes as (
        select FULLNAME, 
            county_fips,
            regexp_extract(
                    regexp_replace(FULLNAME, '{direction_pattern}', ''), 
                    '{suffix_pattern}') 
                as suffix 
                from roads where rttyp = 'M'
    ),

    counts as (
        select 
            county_fips, 
            count(suffix) cnt 
        from suffixes 
        where suffix = 'Cv' 
        group by county_fips)

select 
    lu.long_name, 
    counts.cnt as cove_count, 
    ST_AsWKB(counties.geometry) as geometry
from counts 
left outer join county_fips_lookup lu 
on counts.county_fips = lu.fips

left outer join counties \
on counties.geoid = counts.county_fips

order by cnt desc
"""

con.sql(
    f"COPY ({cove_count_q}) TO 'data/county_cove_count.json' WITH (FORMAT GDAL, DRIVER 'GeoJSON')"
)

# extract all the coves to a GeoJSON file
con.sql(
    "COPY (SELECT * FROM roads WHERE fullname like '%Cv%') TO 'data/coves.json' WITH (FORMAT GDAL, DRIVER 'GeoJSON')"
)