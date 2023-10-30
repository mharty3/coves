import geopandas as gpd


county_counts = gpd.read_file('data/county_cove_count.json')
print(county_counts.head(10))

"""
           long_name  cove_count
0      Shelby County TN        4525
1      Travis County TX        1945
2  Williamson County TX        1231
3      DeSoto County MS        1181
4   Salt Lake County UT         669
5     Pulaski County AR         538
6     Madison County TN         537
7      Rankin County MS         400
8       Allen County IN         329
9       Hinds County MS         307
"""


print(county_counts.cove_count.sum())
# 26051


mid_south = (county_counts
                .dropna()
                [county_counts.dropna()
                ['long_name']
                .str
                .contains('TN|MS|AR')]
                .cx[-93:-87.5, 32.1:]
)
print(mid_south.cove_count.sum() / county_counts.cove_count.sum())
# 0.445

memphis_metro = (county_counts
                    .dropna()
                    [county_counts.dropna()
                    ['long_name']
                    .isin(['Shelby County TN', 'DeSoto County MS'])]
)
print(memphis_metro.cove_count.sum() / county_counts.cove_count.sum())
# 0.219