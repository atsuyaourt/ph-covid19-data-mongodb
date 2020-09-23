import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

import pandas as pd

from helpers import prep_data
from models import CASE_SCHEMA, REGION_MAP, REGION_UNKNOWN
from make_mappable import update_loc_city_mun, update_loc_province


def main():
    load_dotenv()
    mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
    mongo_db = mongo_client["default"]
    mongo_col = mongo_db["cases"]

    in_csvs = list(Path("input/csv").glob("*case_info.csv"))
    in_csvs.sort()
    in_csv = in_csvs[-1]
    curr_df = pd.read_csv(in_csv)
    curr_df = prep_data(curr_df)

    date_str = in_csv.name.split("_")[0]
    new_date = pd.to_datetime(date_str).tz_localize("Asia/Manila")
    print("Date: {}".format(new_date))

    in_csv0 = in_csvs[-2]
    prev_df = pd.read_csv(in_csv0)
    prev_df = prep_data(prev_df)

    new_cols = list(set(curr_df.columns) - set(prev_df.columns))
    if not all([col_name in CASE_SCHEMA.keys() for col_name in new_cols]):
        print("New columns found, please update")
        mongo_client.close()
        sys.exit()

    if len(new_cols) > 0:
        print("Added new columns")
        defaults = {k: v["default"] for col_name in new_cols for k, v in CASE_SCHEMA.items() if k == col_name}
        mongo_col.update_many({}, {"$set": defaults})

    curr_cnt = mongo_col.aggregate(
        [
            {"$match": {"healthStatus": {"$ne": "invalid"}}},
            {"$sort": {"createdAt": -1}},
            {"$group": {"_id": "$caseCode"}},
            {"$group": {"_id": 1, "count": {"$sum": 1}}},
        ]
    ).next()["count"]
    print("Current count: {}".format(curr_cnt))

    common_cols = list(set(prev_df.columns) & set(curr_df.columns))
    new_df = pd.concat([prev_df[common_cols], prev_df[common_cols], curr_df[common_cols]]).drop_duplicates(keep=False)
    new_df = curr_df.loc[curr_df["caseCode"].isin(new_df["caseCode"])].copy()

    new_df.loc[:, "regionResGeo"] = new_df["regionRes"].map(REGION_MAP)
    new_with_city_mun_df = new_df.loc[
        (~((new_df["cityMunRes"] == "") | (new_df["cityMunRes"].isna())))
        & (~(new_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_city_mun_idx = new_with_city_mun_df.index.to_list()
    new_with_city_mun_df = update_loc_city_mun(new_with_city_mun_df)

    new_with_prov_df = new_df.loc[
        (~(new_df.index.isin(with_city_mun_idx)))
        & (~((new_df["provRes"] == "") | (new_df["provRes"].isna())))
        & (~(new_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_prov_idx = new_with_prov_df.index.to_list()
    new_with_prov_df = update_loc_province(new_with_prov_df)

    new_df = pd.concat(
        [
            new_with_city_mun_df,
            new_with_prov_df,
            new_df.loc[~(new_df.index.isin(with_city_mun_idx + with_prov_idx))].copy(),
        ]
    )

    # region updated entries
    exist_df = pd.DataFrame(
        mongo_col.find(
            {"caseCode": {"$in": new_df["caseCode"].to_list()}, "healthStatus": {"$ne": "invalid"}},
            {"caseCode": 1, "healthStatus": 1, "createdAt": 1},
        )
    )

    update_df = exist_df.merge(new_df, on=["caseCode", "healthStatus"])
    if update_df.shape[0] > 0:
        update_ids = update_df["_id"].to_list()
        mongo_col.delete_many({"_id": {"$in": update_ids}})
        update_df["updatedAt"] = new_date
        update_df = update_df.drop(columns=["_id"], errors="ignore")
        data_dict = update_df.to_dict("records")
        mongo_col.insert_many(data_dict)
        print("Updated entries: {}".format(update_df.shape[0]))
    # endregion updated entries

    # region new entries
    if update_df.shape[0] > 0:
        new_df = new_df.loc[
            ~(new_df["caseCode"] + "_" + new_df["healthStatus"]).isin(
                update_df["caseCode"] + "_" + update_df["healthStatus"]
            )
        ].copy()

    if new_df.shape[0] > 0:
        new_df["createdAt"] = new_date
        data_dict = new_df.to_dict("records")
        mongo_col.insert_many(data_dict)
        print("New entries: {}".format(new_df.shape[0]))
    # endregion new entries

    del_case_code = list(set(prev_df["caseCode"]) - set(curr_df["caseCode"]))
    if len(del_case_code) > 0:
        mongo_col.update_many(
            {"caseCode": {"$in": del_case_code}},
            {
                "$set": {
                    "deletedAt": new_date,
                    "removalType": "duplicate",
                    "healthStatus": "invalid",
                }
            },
        )
        print("Deleted entries: {}".format(len(del_case_code)))

    new_cnt = mongo_col.aggregate(
        [
            {"$match": {"healthStatus": {"$ne": "invalid"}}},
            {"$sort": {"createdAt": -1}},
            {"$group": {"_id": "$caseCode"}},
            {"$group": {"_id": 1, "count": {"$sum": 1}}},
        ]
    ).next()["count"]

    print("New CSV count: {}".format(curr_df.shape[0]))
    print("New DB count: {}".format(new_cnt))

    mongo_client.close()


if __name__ == "__main__":
    main()
