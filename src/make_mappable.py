import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

import pandas as pd
import geopandas as gpd

from fuzzywuzzy import process as fz_process

from models import REGION_MAP, REGION_UNKNOWN


def update_loc_city_mun(db_loc_df, mongo_col):
    muni_city_df = gpd.read_file(Path("input/shp/MuniCity/muni_city.shp"))
    muni_city_df = muni_city_df.loc[
        muni_city_df["type"] != "Waterbody",
    ].copy()
    muni_city_df = muni_city_df.sort_values("region")
    muni_city_df["loc_name"] = (muni_city_df["name"] + " " + muni_city_df["province"]).str.lower()

    db_loc_df["loc_name"] = (db_loc_df["cityMunRes"] + " " + db_loc_df["provRes"]).str.lower()

    tot = db_loc_df.shape[0]
    cnt = 0
    for i, r in db_loc_df.iterrows():
        cnt += 1
        m_loc_name = ""
        m_loc_name, _, _ = fz_process.extract(
            r["loc_name"],
            muni_city_df.loc[muni_city_df["region"] == r["regionResGeo"], "loc_name"],
            limit=1,
        )[0]
        print("{:.2f} %".format(100.0 * cnt / tot))
        if len(m_loc_name) > 0:
            m_prov, m_city_mun = muni_city_df.loc[muni_city_df["loc_name"] == m_loc_name, ["province", "name"]].values[
                0
            ]
            db_loc_df.loc[db_loc_df["loc_name"] == r["loc_name"], "provResGeo"] = m_prov
            db_loc_df.loc[db_loc_df["loc_name"] == r["loc_name"], "cityMunResGeo"] = m_city_mun
            mongo_col.update_many(
                {"regionRes": r["regionRes"], "provRes": r["provRes"], "cityMunRes": r["cityMunRes"]},
                {"$set": {"regionResGeo": r["regionResGeo"], "provResGeo": m_prov, "cityMunResGeo": m_city_mun}},
            )


def update_loc_province(db_loc_df, mongo_col):
    prov_df = gpd.read_file(Path("input/shp/Province/province.shp"))
    prov_df = prov_df.sort_values("region")

    tot = db_loc_df.shape[0]
    cnt = 0
    for i, r in db_loc_df.iterrows():
        cnt += 1
        m_loc_name = ""
        m_loc_name, _, _ = fz_process.extract(
            r["provRes"],
            prov_df.loc[prov_df["region"] == r["regionResGeo"], "province"],
            limit=1,
        )[0]
        print("{:.2f} %".format(100.0 * cnt / tot))
        if len(m_loc_name) > 0:
            m_prov = prov_df.loc[prov_df["province"] == m_loc_name, "province"].values[0]
            db_loc_df.loc[db_loc_df["provRes"] == r["provRes"], "provResGeo"] = m_prov
            mongo_col.update_many(
                {"regionRes": r["regionRes"], "provRes": r["provRes"], "cityMunRes": ""},
                {"$set": {"regionResGeo": r["regionResGeo"], "provResGeo": m_prov}},
            )
            mongo_col.update_many(
                {"regionRes": r["regionRes"], "provRes": r["provRes"], "cityMunRes": {"$exists": False}},
                {"$set": {"regionResGeo": r["regionResGeo"], "provResGeo": m_prov}},
            )


def update_loc_region(db_loc_region_df, mongo_col):
    tot = db_loc_region_df.shape[0]
    cnt = 0
    for i, r in db_loc_region_df.iterrows():
        cnt += 1
        print("{:.2f} %".format(100.0 * cnt / tot))
        mongo_col.update_many(
            {"regionRes": r["regionRes"], "provRes": "", "cityMunRes": ""},
            {"$set": {"regionResGeo": r["regionResGeo"]}},
        )
        mongo_col.update_many(
            {"regionRes": r["regionRes"], "provRes": "", "cityMunRes": {"$exists": False}},
            {"$set": {"regionResGeo": r["regionResGeo"]}},
        )
        mongo_col.update_many(
            {"regionRes": r["regionRes"], "provRes": {"$exists": False}, "cityMunRes": ""},
            {"$set": {"regionResGeo": r["regionResGeo"]}},
        )
        mongo_col.update_many(
            {"regionRes": r["regionRes"], "provRes": {"$exists": False}, "cityMunRes": {"$exists": False}},
            {"$set": {"regionResGeo": r["regionResGeo"]}},
        )


def make_mappable():
    load_dotenv()
    mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
    mongo_db = mongo_client["default"]
    mongo_col = mongo_db["cases"]

    # Fetch location data from db

    # db_loc_df.loc[
    #     (db_loc_df["provRes"] == "NCR") & (db_loc_df["cityMunRes"] == "CITY OF CALOOCAN") & (db_loc_df["regionRes"] == ""),
    #     "regionResGeo",
    # ] = "Metropolitan Manila"

    db_loc_city_mun_df = pd.DataFrame(
        mongo_col.find(
            {
                "$and": [
                    {"cityMunRes": {"$exists": True}},
                    {"cityMunRes": {"$ne": ""}},
                    {"$or": [{"cityMunResGeo": {"$exists": False}}, {"cityMunResGeo": ""}]},
                ]
            },
            {"_id": 0, "regionRes": 1, "provRes": 1, "cityMunRes": 1},
        )
    ).drop_duplicates()
    db_loc_city_mun_df.loc[:, "regionResGeo"] = db_loc_city_mun_df["regionRes"].map(REGION_MAP)
    db_loc_city_mun_df = db_loc_city_mun_df.loc[
        ~((db_loc_city_mun_df["regionResGeo"].isin(REGION_UNKNOWN)) | (db_loc_city_mun_df["regionResGeo"].isna()))
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
                    {"$or": [{"cityMunResGeo": {"$exists": False}}, {"cityMunResGeo": ""}]},
                ]
            },
            {"_id": 0, "regionRes": 1, "provRes": 1},
        )
    ).drop_duplicates()
    db_loc_prov_df.loc[:, "regionResGeo"] = db_loc_prov_df["regionRes"].map(REGION_MAP)
    db_loc_prov_df = db_loc_prov_df.loc[
        ~((db_loc_prov_df["regionResGeo"].isin(REGION_UNKNOWN)) | (db_loc_prov_df["regionResGeo"].isna()))
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
                    {"$or": [{"regionResGeo": {"$exists": False}}, {"regionResGeo": ""}]},
                ]
            },
            {"_id": 0, "regionRes": 1},
        )
    ).drop_duplicates()
    db_loc_region_df.loc[:, "regionResGeo"] = db_loc_region_df["regionRes"].map(REGION_MAP)
    db_loc_region_df = db_loc_region_df.loc[
        ~((db_loc_region_df["regionResGeo"].isin(REGION_UNKNOWN)) | (db_loc_region_df["regionResGeo"].isna()))
    ].copy()
    update_loc_region(db_loc_region_df, mongo_col)


if __name__ == "__main__":
    make_mappable()
