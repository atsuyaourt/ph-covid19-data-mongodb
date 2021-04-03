import sys
from pathlib import Path
from pymongo import MongoClient

import pandas as pd
import geopandas as gpd

from constants import MONGO_DB_URL


def main():
    # Load initial data
    in_shp = Path("input/shp/MuniCity") / "muni_city.shp"
    if not in_shp.is_file():
        print("Error: Input file missing")
        sys.exit()
    in_gdf = gpd.read_file(in_shp)
    # prep data
    in_df = in_gdf.loc[
        in_gdf["type"] != "Waterbody", ["region", "province", "name"]
    ].copy()

    region_only_df = in_df.groupby(["region"]).first().reset_index()
    region_only_df["province"] = None
    region_only_df["name"] = None
    region_only_df["type"] = "region"

    province_only_df = in_df.groupby(["region", "province"]).first().reset_index()
    province_only_df["name"] = None
    province_only_df["type"] = "province"

    in_df["type"] = "muni_city"

    out_df = pd.concat([region_only_df, province_only_df, in_df])
    out_df = out_df.rename(columns={"name": "muniCity"})

    # region mongodb
    print("Connecting to mongodb...")
    mongo_client = MongoClient(MONGO_DB_URL)
    mongo_db = mongo_client["defaultDb"]
    mongo_col = mongo_db["ph_loc"]
    if "ph_loc" in mongo_db.list_collection_names():
        # drop collection first
        print("Removing old data...")
        mongo_col.drop()
    print("Connection successful...")
    # endregion mongodb

    # insert data
    data_dict = out_df.to_dict("records")
    mongo_col.insert_many(data_dict)


if __name__ == "__main__":
    main()
