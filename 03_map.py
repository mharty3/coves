import geopandas as gpd
import folium

coves = gpd.read_file('data/coves.json')

map = folium.Map(location=[35.09, -89.91], zoom_start=10)
folium.GeoJson(coves).add_to(map)

map.save('index.html')