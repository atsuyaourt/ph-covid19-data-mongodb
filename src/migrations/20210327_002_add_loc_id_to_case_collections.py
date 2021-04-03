from tqdm import tqdm
from pymongo import MongoClient

import pandas as pd

from constants import MONGO_DB_URL
from models import REGION_MAP, REGION_UNKNOWN
from make_mappable import update_loc_city_mun, update_loc_province, update_loc_region


def main():
    # region mongodb
    print("Connecting to mongodb...")
    mongo_client = MongoClient(MONGO_DB_URL)
    mongo_db = mongo_client["defaultDb"]
    mongo_col = mongo_db["cases"]
    print("Connection successful...")
    # endregion mongodb

    in_df = pd.DataFrame(
        list(
            mongo_col.aggregate(
                [
                    {
                        "$group": {
                            "_id": {
                                "regionRes": "$regionRes",
                                "provRes": "$provRes",
                                "cityMunRes": "$cityMunRes",
                            }
                        }
                    },
                    {
                        "$project": {
                            "regionRes": "$_id.regionRes",
                            "provRes": "$_id.provRes",
                            "cityMunRes": "$_id.cityMunRes",
                            "_id": 0,
                        }
                    },
                ]
            )
        )
    )
    in_df.loc[:, "regionResGeo"] = in_df["regionRes"].map(REGION_MAP)
    with_city_mun_df = in_df.loc[
        (~((in_df["cityMunRes"] == "") | (in_df["cityMunRes"].isna())))
        & (~(in_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_city_mun_df = update_loc_city_mun(with_city_mun_df)

    print("writing to database...")
    mongo_col = mongo_db["cases"]
    for i, r in tqdm(with_city_mun_df.iterrows(), total=with_city_mun_df.shape[0]):
        mongo_col.update_many(
            {
                "regionRes": r["regionRes"],
                "provRes": r["provRes"],
                "cityMunRes": r["cityMunRes"],
            },
            {"$set": {"locId": r["locId"]}},
        )

    in_df = pd.DataFrame(
        list(
            mongo_col.aggregate(
                [
                    {"$match": {"locId": {"$exists": 0}}},
                    {
                        "$group": {
                            "_id": {"regionRes": "$regionRes", "provRes": "$provRes"}
                        }
                    },
                    {
                        "$project": {
                            "regionRes": "$_id.regionRes",
                            "provRes": "$_id.provRes",
                            "_id": 0,
                        }
                    },
                ]
            )
        )
    )
    in_df.loc[:, "regionResGeo"] = in_df["regionRes"].map(REGION_MAP)
    with_prov_df = in_df.loc[
        (~((in_df["provRes"] == "") | (in_df["provRes"].isna())))
        & (~(in_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_prov_df = update_loc_province(with_prov_df)

    print("writing to database...")
    mongo_col = mongo_db["cases"]
    for i, r in tqdm(with_prov_df.iterrows(), total=with_prov_df.shape[0]):
        mongo_col.update_many(
            {
                "regionRes": r["regionRes"],
                "provRes": r["provRes"],
            },
            {"$set": {"locId": r["locId"]}},
        )

    in_df = pd.DataFrame(
        list(
            mongo_col.aggregate(
                [
                    {"$match": {"locId": {"$exists": 0}}},
                    {"$group": {"_id": "$regionRes"}},
                    {
                        "$project": {
                            "regionRes": "$_id",
                            "_id": 0,
                        }
                    },
                ]
            )
        )
    )
    in_df.loc[:, "regionResGeo"] = in_df["regionRes"].map(REGION_MAP)
    with_region_df = in_df.loc[
        (~((in_df["regionResGeo"] == "") | (in_df["regionResGeo"].isna())))
        & (~(in_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_region_df = update_loc_region(with_region_df)

    print("writing to database...")
    mongo_col = mongo_db["cases"]
    for i, r in tqdm(with_region_df.iterrows(), total=with_region_df.shape[0]):
        mongo_col.update_many(
            {"regionRes": r["regionRes"]},
            {"$set": {"locId": r["locId"]}},
        )


if __name__ == "__main__":
    main()
