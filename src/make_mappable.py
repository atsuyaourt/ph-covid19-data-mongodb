import os
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

import pandas as pd
import geopandas as gpd

from fuzzywuzzy import process as fz_process

from models import REGION_MAP, REGION_UNKNOWN

LOC_CITY_MUN_SAV = Path("config/lookup/loc_city_mun.csv")
LOC_PROV_SAV = Path("config/lookup/loc_prov.csv")


def update_loc_city_mun(db_loc_df, mongo_col=None):
    muni_city_df = gpd.read_file(Path("input/shp/MuniCity/muni_city.shp"))
    muni_city_df = muni_city_df.loc[
        muni_city_df["type"] != "Waterbody",
    ].copy()
    muni_city_df = muni_city_df.sort_values("region")
    muni_city_df["loc_name"] = (muni_city_df["name"] + " " + muni_city_df["province"]).str.lower()

    db_loc_df["loc_name"] = (
        (db_loc_df["cityMunRes"] + " " + db_loc_df["provRes"]).str.lower().str.replace(r"\([^)]*\)\ ", "", regex=True)
    )

    lookup_df = pd.DataFrame(
        [{"regionRes": "", "provRes": "", "cityMunRes": "", "provResGeo": "", "cityMunResGeo": ""}]
    )
    if LOC_CITY_MUN_SAV.is_file():
        lookup_df = pd.read_csv(LOC_CITY_MUN_SAV)

    for i, r in tqdm(db_loc_df.iterrows(), total=db_loc_df.shape[0]):
        res_prov = ""
        res_city_mun = ""
        res = lookup_df.loc[
            (lookup_df["regionRes"] == r["regionRes"])
            & (lookup_df["provRes"] == r["provRes"])
            & (lookup_df["cityMunRes"] == r["cityMunRes"]),
            ["provResGeo", "cityMunResGeo"],
        ]
        if res.shape[0] == 0:
            res, _, _ = fz_process.extract(
                r["loc_name"],
                muni_city_df.loc[muni_city_df["region"] == r["regionResGeo"], "loc_name"],
                limit=1,
            )[0]
            if len(res) > 0:
                res_prov, res_city_mun = muni_city_df.loc[muni_city_df["loc_name"] == res, ["province", "name"]].values[
                    0
                ]
        else:
            res_prov, res_city_mun = res.iloc[0].values
        db_loc_df.loc[db_loc_df["loc_name"] == r["loc_name"], "provResGeo"] = res_prov
        db_loc_df.loc[db_loc_df["loc_name"] == r["loc_name"], "cityMunResGeo"] = res_city_mun
        if (mongo_col is not None) & (res_prov != "") & (res_city_mun != ""):
            mongo_col.update_many(
                {"regionRes": r["regionRes"], "provRes": r["provRes"], "cityMunRes": r["cityMunRes"]},
                {"$set": {"regionResGeo": r["regionResGeo"], "provResGeo": res_prov, "cityMunResGeo": res_city_mun}},
            )
    if mongo_col is None:
        (
            pd.concat(
                [
                    lookup_df,
                    db_loc_df[["regionRes", "provRes", "cityMunRes", "regionResGeo", "provResGeo", "cityMunResGeo"]],
                ],
                ignore_index=True,
            )
            .drop_duplicates()
            .dropna()
        ).to_csv(LOC_CITY_MUN_SAV, index=False)
        return db_loc_df.drop(columns=["loc_name"], errors="ignore")


def update_loc_province(db_loc_df, mongo_col=None):
    prov_df = gpd.read_file(Path("input/shp/Province/province.shp"))
    prov_df = prov_df.sort_values("region")

    lookup_df = pd.DataFrame([{"regionRes": "", "provRes": "", "provResGeo": ""}])
    if LOC_PROV_SAV.is_file():
        lookup_df = pd.read_csv(LOC_PROV_SAV)

    for i, r in tqdm(db_loc_df.iterrows(), total=db_loc_df.shape[0]):
        res_prov = ""
        res = lookup_df.loc[
            (lookup_df["regionRes"] == r["regionRes"]) & (lookup_df["provRes"] == r["provRes"]), ["provResGeo"]
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
            res_prov = res.iloc[0].values
        db_loc_df.loc[db_loc_df["provRes"] == r["provRes"], "provResGeo"] = res_prov
        if (mongo_col is not None) & (res_prov != ""):
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
