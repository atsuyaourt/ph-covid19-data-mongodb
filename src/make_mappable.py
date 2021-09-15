from dotenv import dotenv_values
from tqdm import tqdm
from pathlib import Path

import pandas as pd
import geopandas as gpd

from fuzzywuzzy import process as fz_process

from models import REGION_MAP, REGION_UNKNOWN

LOC_CITY_MUN_SAV = Path("config/lookup/loc_city_mun.csv")
LOC_PROV_SAV = Path("config/lookup/loc_prov.csv")
config = dotenv_values()


def update_loc_city_mun(db_loc_df):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo", "provRes", "cityMunRes"]

    Returns:
        pandas.DataFrame: The updated data.
    """
    muni_city_gdf = gpd.read_file("input/shp/Municipalities/Municipalities.shp")
    muni_city_gdf.rename(
        columns={"ADM3_EN": "muniCity", "ADM2_EN": "province", "ADM1_EN": "region"},
        inplace=True,
    )
    muni_city_gdf = muni_city_gdf.sort_values("region")
    muni_city_gdf["loc_name"] = (
        muni_city_gdf["muniCity"] + " " + muni_city_gdf["province"]
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
                "psgc": "",
            }
        ]
    )
    if LOC_CITY_MUN_SAV.is_file():
        lookup_df = pd.read_csv(LOC_CITY_MUN_SAV)

    print("matching location name...")
    for i, r in tqdm(_db_loc_df.iterrows(), total=_db_loc_df.shape[0]):
        res_psgc = ""
        res = lookup_df.loc[
            (lookup_df["regionResGeo"] == r["regionResGeo"])
            & (lookup_df["provRes"] == r["provRes"])
            & (lookup_df["cityMunRes"] == r["cityMunRes"]),
            "psgc",
        ]
        if res.shape[0] == 0:
            res, _, _ = fz_process.extract(
                r["loc_name"],
                muni_city_gdf.loc[
                    muni_city_gdf["region"] == r["regionResGeo"], "loc_name"
                ],
                limit=1,
            )[0]
            if len(res) > 0:
                res_psgc = muni_city_gdf.loc[
                    muni_city_gdf["loc_name"] == res, "ADM3_PCODE"
                ].values[0]
        else:
            res_psgc = res.values[0]
        db_loc_df.loc[db_loc_df["loc_name"] == r["loc_name"], "cityMuniPSGC"] = res_psgc
        db_loc_df.loc[db_loc_df["loc_name"] == r["loc_name"], "psgc"] = res_psgc

    pd.concat(
        [
            lookup_df,
            db_loc_df[["regionRes", "provRes", "cityMunRes", "regionResGeo", "psgc"]],
        ],
        ignore_index=True,
    ).drop_duplicates().dropna().to_csv(LOC_CITY_MUN_SAV, index=False)
    return db_loc_df.drop(columns=["loc_name", "regionResGeo", "psgc"], errors="ignore")


def update_loc_province(db_loc_df):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo", "provRes"]

    Returns:
        pandas.DataFrame: The updated data.
    """
    prov_gdf = gpd.read_file("input/shp/Provinces/Provinces.shp")
    prov_gdf.rename(
        columns={"ADM2_EN": "province", "ADM1_EN": "region"},
        inplace=True,
    )
    prov_gdf = prov_gdf.sort_values("region")

    _db_loc_df = db_loc_df.drop_duplicates(
        subset=[
            "regionRes",
            "provRes",
            "regionResGeo",
        ]
    )

    lookup_df = pd.DataFrame(
        [{"regionRes": "", "provRes": "", "regionResGeo": "", "psgc": ""}]
    )
    if LOC_PROV_SAV.is_file():
        lookup_df = pd.read_csv(LOC_PROV_SAV)

    print("matching location name...")
    for i, r in tqdm(_db_loc_df.iterrows(), total=_db_loc_df.shape[0]):
        res_psgc = ""
        res = lookup_df.loc[
            (lookup_df["regionRes"] == r["regionRes"])
            & (lookup_df["provRes"] == r["provRes"]),
            "psgc",
        ]
        if res.shape[0] == 0:
            res, _, _ = fz_process.extract(
                r["provRes"],
                prov_gdf.loc[prov_gdf["region"] == r["regionResGeo"], "province"],
                limit=1,
            )[0]
            if len(res) > 0:
                res_psgc = prov_gdf.loc[
                    prov_gdf["province"] == res, "ADM2_PCODE"
                ].values[0]
        else:
            res_psgc = res.values[0]
        db_loc_df.loc[db_loc_df["provRes"] == r["provRes"], "cityMuniPSGC"] = res_psgc
        db_loc_df.loc[db_loc_df["provRes"] == r["provRes"], "psgc"] = res_psgc

    pd.concat(
        [
            lookup_df,
            db_loc_df[["regionRes", "provRes", "regionResGeo", "psgc"]],
        ],
        ignore_index=True,
    ).drop_duplicates().dropna().to_csv(LOC_PROV_SAV, index=False)
    return db_loc_df.drop(columns=["regionResGeo", "psgc"], errors="ignore")


def update_loc_region(db_loc_df):
    """Add mappable columns to DataFrame

    Args:
        db_loc_df (pandas.DataFrame): Input data. Should contain the following columns:
                ["regionResGeo"]
        mongo_col (pymongo.Collection): The collection to be updated.
    """
    region_gdf = gpd.read_file("input/shp/Regions/Regions.shp")
    region_gdf.rename(
        columns={"ADM1_EN": "region"},
        inplace=True,
    )
    region_gdf = region_gdf.sort_values("region")

    _db_loc_df = db_loc_df.drop_duplicates(subset=["regionResGeo"])

    print("matching location name...")
    for i, r in tqdm(_db_loc_df.iterrows(), total=_db_loc_df.shape[0]):
        res_psgc = region_gdf.loc[
            region_gdf["region"] == r["regionResGeo"],
            "ADM1_PCODE",
        ].values[0]
        db_loc_df.loc[
            db_loc_df["regionResGeo"] == r["regionResGeo"], "cityMuniPSGC"
        ] = res_psgc
    return db_loc_df.drop(columns=["regionResGeo"], errors="ignore")


def make_mappable(df):
    _df = df.copy()

    _df.loc[:, "regionResGeo"] = _df["regionRes"].map(REGION_MAP)
    no_loc_df = _df.loc[_df["regionResGeo"].isin(REGION_UNKNOWN)].copy()
    _df = _df.loc[~_df["regionResGeo"].isin(REGION_UNKNOWN)].copy()

    with_city_mun_df = _df.loc[
        ~((_df["cityMunRes"] == "") | (_df["cityMunRes"].isna()))
    ].copy()
    with_city_mun_idx = with_city_mun_df.index.to_list()
    if with_city_mun_df.shape[0] > 0:
        with_city_mun_df = update_loc_city_mun(with_city_mun_df)
    _df = _df.loc[~_df.index.isin(with_city_mun_idx)].copy()

    with_prov_df = _df.loc[~((_df["provRes"] == "") | (_df["provRes"].isna()))].copy()
    with_prov_idx = with_prov_df.index.to_list()
    if with_prov_df.shape[0] > 0:
        with_prov_df = update_loc_province(with_prov_df)
    _df = _df.loc[~_df.index.isin(with_prov_idx)].copy()

    if _df.shape[0] > 0:
        _df = update_loc_region(_df)

    if no_loc_df.shape[0] > 0:
        print(no_loc_df["regionRes"].unique())
    no_loc_df.drop(columns=["regionResGeo"], errors="ignore", inplace=True)

    return pd.concat(
        [
            with_city_mun_df,
            with_prov_df,
            _df,
            no_loc_df,
        ]
    ).drop(columns=["regionResGeo"], errors="ignore")


if __name__ == "__main__":
    make_mappable()
