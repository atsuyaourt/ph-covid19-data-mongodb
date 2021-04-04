import sys
import json
from pymongo import MongoClient
from tqdm import tqdm

import geopandas as gpd
from fuzzywuzzy import process as fz_process

from constants import MONGO_DB_URL
from models import REGION_MAP


in_gdf = gpd.read_file("input/geojson/ph-prov.geojson")

in_gdf2 = gpd.read_file("input/shp/PopDen/Population Density.shp")
in_gdf2.columns = in_gdf2.columns.str.lower()
in_gdf2["region"] = in_gdf2["region"].map(REGION_MAP)
in_gdf2 = in_gdf2.rename(
    columns={
        "name_1": "province",
        "popden2015": "popDen2015",
        "grate_2015": "growthRate2015",
    }
)

for i, r in tqdm(in_gdf2.iterrows(), total=in_gdf2.shape[0]):
    res, _, _ = fz_process.extract(
        r["province"],
        in_gdf.loc[in_gdf["region"] == r["region"], "province"],
        limit=1,
    )[0]
    if len(res) > 0:
        in_gdf2.loc[in_gdf2["province"] == r["province"], "province"] = res

out_gdf = in_gdf.merge(
    in_gdf2[["region", "province", "area", "popDen2015", "growthRate2015"]],
    on=["region", "province"],
)

out_gdf.to_file("input/geojson/ph-prov-popden.geojson", driver="GeoJSON")


# region mongodb
print("Connecting to mongodb...")
mongo_client = MongoClient(MONGO_DB_URL)
if "default" not in mongo_client.list_database_names():
    print("Database not found... exiting...")
    mongo_client.close()
    sys.exit()
mongo_db = mongo_client["defaultDb"]
mongo_col = mongo_db["geomaps"]
print("Connection successful...")
# endregion mongodb

mongo_col.delete_one({"name": "ph-prov"})

with open("input/geojson/ph-prov-popden.geojson") as file:
    geo_data = json.load(file)
data_dict = dict(name="ph-prov", geo=geo_data)

mongo_col.insert_one(data_dict)