import os
import sys
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

import pandas as pd
import geopandas as gpd

from fuzzywuzzy import process as fz_process

from models import REGION_MAP, REGION_UNKNOWN

load_dotenv()

LOC_CITY_MUN_SAV = Path("config/lookup/loc_city_mun.csv")
LOC_PROV_SAV = Path("config/lookup/loc_prov.csv")


def update_loc_city_mun(db_loc_df, mongo_col=None):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo", "provRes", "cityMunRes"]
        mongo_col (pymongo.Collection, optional): The collection to be updated.
                Defaults to None.

    Returns:
        pandas.DataFrame: The updated data.
    """
    muni_city_df = gpd.read_file(Path("input/shp/MuniCity/muni_city.shp"))
    muni_city_df = muni_city_df.loc[
        muni_city_df["type"] != "Waterbody",
    ].copy()
    muni_city_df = muni_city_df.sort_values("region")
    muni_city_df["loc_name"] = (
        muni_city_df["name"] + " " + muni_city_df["province"]
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
                "provResGeo": "",
                "cityMunResGeo": "",
            }
        ]
    )
    if LOC_CITY_MUN_SAV.is_file():
        lookup_df = pd.read_csv(LOC_CITY_MUN_SAV)

    print("matching location name...")
    for i, r in tqdm(_db_loc_df.iterrows(), total=_db_loc_df.shape[0]):
        res_prov = ""
        res_city_mun = ""
        res = lookup_df.loc[
            (lookup_df["regionResGeo"] == r["regionResGeo"])
            & (lookup_df["provRes"] == r["provRes"])
            & (lookup_df["cityMunRes"] == r["cityMunRes"]),
            ["provResGeo", "cityMunResGeo"],
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
                res_prov, res_city_mun = muni_city_df.loc[
                    muni_city_df["loc_name"] == res, ["province", "name"]
                ].values[0]
        else:
            res_prov, res_city_mun = res.iloc[0].values
        db_loc_df.loc[db_loc_df["loc_name"] == r["loc_name"], "provResGeo"] = res_prov
        db_loc_df.loc[
            db_loc_df["loc_name"] == r["loc_name"], "cityMunResGeo"
        ] = res_city_mun
        if (mongo_col is not None) & (res_prov != "") & (res_city_mun != ""):
            print("writing to database...")
            mongo_col.update_many(
                {
                    "regionRes": r["regionRes"],
                    "provRes": r["provRes"],
                    "cityMunRes": r["cityMunRes"],
                },
                {
                    "$set": {
                        "regionResGeo": r["regionResGeo"],
                        "provResGeo": res_prov,
                        "cityMunResGeo": res_city_mun,
                    }
                },
            )
    if mongo_col is None:
        (
            pd.concat(
                [
                    lookup_df,
                    db_loc_df[
                        [
                            "regionRes",
                            "provRes",
                            "cityMunRes",
                            "regionResGeo",
                            "provResGeo",
                            "cityMunResGeo",
                        ]
                    ],
                ],
                ignore_index=True,
            )
            .drop_duplicates()
            .dropna()
        ).to_csv(LOC_CITY_MUN_SAV, index=False)
        return db_loc_df.drop(columns=["loc_name"], errors="ignore")


def update_loc_province(db_loc_df, mongo_col=None):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo", "provRes"]
        mongo_col (pymongo.Collection, optional): The collection to be updated.
                Defaults to None.

    Returns:
        pandas.DataFrame: The updated data.
    """
    prov_df = gpd.read_file(Path("input/shp/Province/province.shp"))
    prov_df = prov_df.sort_values("region")

    _db_loc_df = db_loc_df.drop_duplicates(
        subset=[
            "regionRes",
            "provRes",
            "regionResGeo",
        ]
    )

    lookup_df = pd.DataFrame(
        [{"regionRes": "", "provRes": "", "regionResGeo": "", "provResGeo": ""}]
    )
    if LOC_PROV_SAV.is_file():
        lookup_df = pd.read_csv(LOC_PROV_SAV)

    print("matching location name...")
    for i, r in tqdm(_db_loc_df.iterrows(), total=_db_loc_df.shape[0]):
        res_prov = ""
        res = lookup_df.loc[
            (lookup_df["regionRes"] == r["regionRes"])
            & (lookup_df["provRes"] == r["provRes"]),
            ["provResGeo"],
        ]
        if res.shape[0] == 0:
            res, _, _ = fz_process.extract(
                r["provRes"],
                prov_df.loc[prov_df["region"] == r["regionResGeo"], "province"],
                limit=1,
            )[0]
            if len(res) > 0:
                res_prov = prov_df.loc[prov_df["province"] == res, "province"].values[0]
        else:
            res_prov = res.iloc[0].values[0]
        db_loc_df.loc[db_loc_df["provRes"] == r["provRes"], "provResGeo"] = res_prov
        if (mongo_col is not None) & (res_prov != ""):
            print("writing to database...")
            mongo_col.update_many(
                {
                    "regionRes": r["regionRes"],
                    "provRes": r["provRes"],
                    "$or": [{"cityMunRes": {"$exists": False}}, {"cityMunRes": ""}],
                },
                {"$set": {"regionResGeo": r["regionResGeo"], "provResGeo": res_prov}},
            )
    if mongo_col is None:
        (
            pd.concat(
                [
                    lookup_df,
                    db_loc_df[["regionRes", "provRes", "regionResGeo", "provResGeo"]],
                ],
                ignore_index=True,
            )
            .drop_duplicates()
            .dropna()
        ).to_csv(LOC_PROV_SAV, index=False)
        return db_loc_df


def update_loc_region(db_loc_region_df, mongo_col):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo"]
        mongo_col (pymongo.Collection): The collection to be updated.
    """
    tot = db_loc_region_df.shape[0]
    cnt = 0
    for i, r in db_loc_region_df.iterrows():
        cnt += 1
        print("{:.2f} %".format(100.0 * cnt / tot))
        mongo_col.update_many(
            {
                "$and": [
                    {"regionRes": r["regionRes"]},
                    {"$or": [{"provRes": {"$exists": False}}, {"provRes": ""}]},
                    {"$or": [{"cityMunRes": {"$exists": False}}, {"cityMunRes": ""}]},
                ],
            },
            {"$set": {"regionResGeo": r["regionResGeo"]}},
        )


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
