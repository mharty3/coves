import re

from bs4 import BeautifulSoup
import duckdb
import geopandas as gpd
import pandas as pd
import requests
from tqdm import tqdm


con = duckdb.connect('data/roads.db')

con.install_extension("httpfs")
con.load_extension("httpfs")

con.install_extension("spatial")
con.load_extension("spatial")


r = requests.get('https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/')
soup = BeautifulSoup(r.content, 'html.parser')
links = soup.find_all("a") # Find all elements with the tag <a>

pfix = 'https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/ROADS/'
links = [pfix + link.get("href") for link in links[6:] if link.get('href').startswith('tl_rd22')]


pattern = r'_(\d{5})_'
for link in tqdm(links):
    matches = re.findall(pattern, link)
    gpd.read_file(link).assign(county_fips=matches[0]).to_parquet(f'data/roads/{matches[0]}.parquet')


con.sql(
    """
    CREATE TABLE IF NOT EXISTS roads AS
    SELECT * EXCLUDE geometry, ST_GeomFromWKB(geometry)
    AS geometry FROM 'data/roads/*.parquet'
    """
)


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