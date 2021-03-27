import os
import sys
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId

import pandas as pd
import geopandas as gpd

from fuzzywuzzy import process as fz_process
from models import REGION_MAP, REGION_UNKNOWN

load_dotenv()

LOC_CITY_MUN_SAV = Path("config/lookup/loc_city_mun.csv")
LOC_PROV_SAV = Path("config/lookup/loc_prov.csv")


def update_loc_city_mun(db_loc_df):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo", "provRes", "cityMunRes"]

    Returns:
        pandas.DataFrame: The updated data.
    """
    # region mongodb
    mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
    mongo_db = mongo_client["defaultDb"]
    mongo_col = mongo_db["ph_loc"]
    # endregion mongodb

    muni_city_df = pd.DataFrame(list(mongo_col.find({"type": "muni_city"})))
    muni_city_df = muni_city_df.sort_values("region")
    muni_city_df["loc_name"] = (
        muni_city_df["muniCity"] + " " + muni_city_df["province"]
    ).str.lower()

    db_loc_df["loc_name"] = (
        (db_loc_df["cityMunRes"] + " " + db_loc_df["provRes"])
        .str.lower()
        .str.replace(r"\([^)]*\)\ ", "", regex=True)
    )

    _db_loc_df = db_loc_df.drop_duplicates(
        subset=[
            "regionRes",
            "provRes",
            "cityMunRes",
            "regionResGeo",
        ]
    )

    lookup_df = pd.DataFrame(
        [
            {
                "regionRes": "",
                "provRes": "",
                "cityMunRes": "",
                "regionResGeo": "",
                "locId": "",
            }
        ]
    )
    if LOC_CITY_MUN_SAV.is_file():
        lookup_df = pd.read_csv(LOC_CITY_MUN_SAV)
        lookup_df["locId"] = lookup_df["locId"].map(ObjectId, na_action="ignore")

    print("matching location name...")
    for i, r in tqdm(_db_loc_df.iterrows(), total=_db_loc_df.shape[0]):
        res_loc_id = ""
        res = lookup_df.loc[
            (lookup_df["regionResGeo"] == r["regionResGeo"])
            & (lookup_df["provRes"] == r["provRes"])
            & (lookup_df["cityMunRes"] == r["cityMunRes"]),
            "locId",
        ]
        if res.shape[0] == 0:
            res, _, _ = fz_process.extract(
                r["loc_name"],
                muni_city_df.loc[
                    muni_city_df["region"] == r["regionResGeo"], "loc_name"
                ],
                limit=1,
            )[0]
            if len(res) > 0:
                res_loc_id = muni_city_df.loc[
                    muni_city_df["loc_name"] == res, "_id"
                ].values[0]
        else:
            res_loc_id = res.values[0]
        db_loc_df.loc[db_loc_df["loc_name"] == r["loc_name"], "locId"] = res_loc_id

    pd.concat(
        [
            lookup_df,
            db_loc_df[["regionRes", "provRes", "cityMunRes", "regionResGeo", "locId"]],
        ],
        ignore_index=True,
    ).drop_duplicates().dropna().to_csv(LOC_CITY_MUN_SAV, index=False)
    return db_loc_df.drop(columns=["loc_name", "regionResGeo"], errors="ignore")


def update_loc_province(db_loc_df):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo", "provRes"]

    Returns:
        pandas.DataFrame: The updated data.
    """
    # region mongodb
    mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
    mongo_db = mongo_client["defaultDb"]
    mongo_col = mongo_db["ph_loc"]
    # endregion mongodb

    prov_df = pd.DataFrame(list(mongo_col.find({"type": "province"})))
    prov_df = prov_df.sort_values("region")

    _db_loc_df = db_loc_df.drop_duplicates(
        subset=[
            "regionRes",
            "provRes",
            "regionResGeo",
        ]
    )

    lookup_df = pd.DataFrame(
        [{"regionRes": "", "provRes": "", "regionResGeo": "", "locId": ""}]
    )
    if LOC_PROV_SAV.is_file():
        lookup_df = pd.read_csv(LOC_PROV_SAV)
        lookup_df["locId"] = lookup_df["locId"].map(ObjectId, na_action="ignore")

    print("matching location name...")
    for i, r in tqdm(_db_loc_df.iterrows(), total=_db_loc_df.shape[0]):
        res_loc_id = ""
        res = lookup_df.loc[
            (lookup_df["regionRes"] == r["regionRes"])
            & (lookup_df["provRes"] == r["provRes"]),
            "locId",
        ]
        if res.shape[0] == 0:
            res, _, _ = fz_process.extract(
                r["provRes"],
                prov_df.loc[prov_df["region"] == r["regionResGeo"], "province"],
                limit=1,
            )[0]
            if len(res) > 0:
                res_loc_id = prov_df.loc[prov_df["province"] == res, "_id"].values[0]
        else:
            res_loc_id = res.values[0]
        db_loc_df.loc[db_loc_df["provRes"] == r["provRes"], "locId"] = res_loc_id

    pd.concat(
        [
            lookup_df,
            db_loc_df[["regionRes", "provRes", "regionResGeo", "locId"]],
        ],
        ignore_index=True,
    ).drop_duplicates().dropna().to_csv(LOC_PROV_SAV, index=False)
    return db_loc_df.drop(columns=["regionResGeo"], errors="ignore")


def update_loc_region(db_loc_df):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo"]
        mongo_col (pymongo.Collection): The collection to be updated.
    """
    # region mongodb
    mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
    mongo_db = mongo_client["defaultDb"]
    mongo_col = mongo_db["ph_loc"]
    # endregion mongodb

    region_df = pd.DataFrame(list(mongo_col.find({"type": "region"})))
    region_df = region_df.sort_values("region")

    _db_loc_df = db_loc_df.drop_duplicates(subset=["regionResGeo"])

    print("matching location name...")
    for i, r in tqdm(_db_loc_df.iterrows(), total=_db_loc_df.shape[0]):
        res_loc_id = region_df.loc[
            region_df["region"] == r["regionResGeo"],
            "_id",
        ].values[0]
        db_loc_df.loc[
            db_loc_df["regionResGeo"] == r["regionResGeo"], "locId"
        ] = res_loc_id
    return db_loc_df.drop(columns=["regionResGeo"], errors="ignore")


def make_mappable():
    # region mongodb
    print("Connecting to mongodb...")
    mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
    if "default" not in mongo_client.list_database_names():
        print("Database not found... exiting...")
        mongo_client.close()
        sys.exit()
    mongo_db = mongo_client["default"]
    if "cases" not in mongo_db.list_collection_names():
        print("Collection not found... exiting...")
        mongo_client.close()
        sys.exit()
    mongo_col = mongo_db["cases"]
    print("Connection successful...")
    # endregion mongodb

    db_loc_city_mun_df = pd.DataFrame(
        mongo_col.find(
            {
                "$and": [
                    {"cityMunRes": {"$exists": True}},
                    {"cityMunRes": {"$ne": ""}},
                    {
                        "$or": [
                            {"cityMunResGeo": {"$exists": False}},
                            {"cityMunResGeo": ""},
                        ]
                    },
                ]
            },
            {"_id": 0, "regionRes": 1, "provRes": 1, "cityMunRes": 1},
        )
    ).drop_duplicates()
    db_loc_city_mun_df.loc[:, "regionResGeo"] = db_loc_city_mun_df["regionRes"].map(
        REGION_MAP
    )
    db_loc_city_mun_df = db_loc_city_mun_df.loc[
        ~(
            (db_loc_city_mun_df["regionResGeo"].isin(REGION_UNKNOWN))
            | (db_loc_city_mun_df["regionResGeo"].isna())
        )
    ].copy()
    update_loc_city_mun(db_loc_city_mun_df, mongo_col)

    db_loc_prov_df = pd.DataFrame(
        mongo_col.find(
            {
                "$and": [
                    {"provRes": {"$exists": True}},
                    {"provRes": {"$ne": ""}},
                    {"$or": [{"cityMunRes": {"$exists": False}}, {"cityMunRes": ""}]},
                    {"$or": [{"provResGeo": {"$exists": False}}, {"provResGeo": ""}]},
                    {
                        "$or": [
                            {"cityMunResGeo": {"$exists": False}},
                            {"cityMunResGeo": ""},
                        ]
                    },
                ]
            },
            {"_id": 0, "regionRes": 1, "provRes": 1},
        )
    ).drop_duplicates()
    db_loc_prov_df.loc[:, "regionResGeo"] = db_loc_prov_df["regionRes"].map(REGION_MAP)
    db_loc_prov_df = db_loc_prov_df.loc[
        ~(
            (db_loc_prov_df["regionResGeo"].isin(REGION_UNKNOWN))
            | (db_loc_prov_df["regionResGeo"].isna())
        )
    ].copy()
    update_loc_province(db_loc_prov_df, mongo_col)

    db_loc_region_df = pd.DataFrame(
        mongo_col.find(
            {
                "$and": [
                    {"regionRes": {"$exists": True}},
                    {"regionRes": {"$ne": ""}},
                    {"$or": [{"cityMunRes": {"$exists": False}}, {"cityMunRes": ""}]},
                    {"$or": [{"provRes": {"$exists": False}}, {"provRes": ""}]},
                    {
                        "$or": [
                            {"regionResGeo": {"$exists": False}},
                            {"regionResGeo": ""},
                        ]
                    },
                ]
            },
            {"_id": 0, "regionRes": 1},
        )
    ).drop_duplicates()
    db_loc_region_df.loc[:, "regionResGeo"] = db_loc_region_df["regionRes"].map(
        REGION_MAP
    )
    db_loc_region_df = db_loc_region_df.loc[
        ~(
            (db_loc_region_df["regionResGeo"].isin(REGION_UNKNOWN))
            | (db_loc_region_df["regionResGeo"].isna())
        )
    ].copy()
    update_loc_region(db_loc_region_df, mongo_col)


if __name__ == "__main__":
    make_mappable()
