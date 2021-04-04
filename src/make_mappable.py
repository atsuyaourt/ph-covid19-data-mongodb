import os
import sys
from tqdm import tqdm
from pathlib import Path
from pymongo import MongoClient
from bson.objectid import ObjectId

import pandas as pd
import geopandas as gpd

from fuzzywuzzy import process as fz_process

from constants import MONGO_DB_URL
from models import REGION_MAP, REGION_UNKNOWN

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
    mongo_client = MongoClient(MONGO_DB_URL)
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
    mongo_client = MongoClient(MONGO_DB_URL)
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
    mongo_client = MongoClient(MONGO_DB_URL)
    mongo_db = mongo_client["defaultDb"]
    mongo_col = mongo_db["ph_loc"]
    # endregion mongodb

    region_df = pd.DataFrame(
        list(mongo_col.find({"type": {"$in": ["region", "misc"]}}))
    )
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


def make_mappable(df):
    _df = df.copy()
    _df.loc[:, "regionResGeo"] = _df["regionRes"].map(REGION_MAP)
    with_city_mun_df = _df.loc[
        (~((_df["cityMunRes"] == "") | (_df["cityMunRes"].isna())))
        & (~(_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_city_mun_idx = with_city_mun_df.index.to_list()
    if with_city_mun_df.shape[0] > 0:
        with_city_mun_df = update_loc_city_mun(with_city_mun_df)

    with_prov_df = _df.loc[
        (~(_df.index.isin(with_city_mun_idx)))
        & (~((_df["provRes"] == "") | (_df["provRes"].isna())))
        & (~(_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_prov_idx = with_prov_df.index.to_list()
    if with_prov_df.shape[0] > 0:
        with_prov_df = update_loc_province(with_prov_df)

    with_reg_df = _df.loc[~(_df.index.isin(with_city_mun_idx + with_prov_idx))].copy()
    with_reg_df["regionRes"] = with_reg_df["regionRes"].fillna("")
    with_reg_idx = with_reg_df.index.to_list()
    if with_reg_df.shape[0] > 0:
        with_reg_df = update_loc_region(with_reg_df)

    no_loc_df = _df.loc[
        ~(_df.index.isin(with_city_mun_idx + with_prov_idx + with_reg_idx))
    ].copy()
    if no_loc_df.shape[0] > 0:
        print(no_loc_df["regionRes"].unique())
    no_loc_df = no_loc_df.drop(columns=["regionResGeo"], errors="ignore").copy()

    return (
        pd.concat(
            [
                with_city_mun_df,
                with_prov_df,
                with_reg_df,
                no_loc_df,
            ]
        )
        .drop(columns=["regionResGeo"], errors="ignore")
        .copy()
    )


if __name__ == "__main__":
    make_mappable()
