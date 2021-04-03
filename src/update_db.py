import sys
from pymongo import MongoClient

import pandas as pd

from constants import CASE_INFO_CSV_DIR, MONGO_DB_URL, TZ
from models import CASE_SCHEMA, prep_cases_df
from make_mappable import make_mappable


def main():
    # region mongodb
    print("Connecting to mongodb...")
    mongo_client = MongoClient(MONGO_DB_URL)
    if "defaultDb" not in mongo_client.list_database_names():
        print("Database not found... exiting...")
        mongo_client.close()
        sys.exit()
    mongo_db = mongo_client["defaultDb"]
    if "cases" not in mongo_db.list_collection_names():
        print("Collection not found... exiting...")
        mongo_client.close()
        sys.exit()
    mongo_col = mongo_db["cases"]
    print("Connection successful...")
    # endregion mongodb

    in_csvs = list(CASE_INFO_CSV_DIR.glob("*case_info.csv"))
    in_csvs.sort()
    in_csv = in_csvs[-1]
    curr_df = pd.read_csv(in_csv, low_memory=False)
    curr_df = prep_cases_df(curr_df)

    date_str = in_csv.name.split("_")[0]
    new_date = pd.to_datetime(date_str).tz_localize(TZ)
    print("Date: {}".format(new_date))

    in_csv0 = in_csvs[-2]
    prev_df = pd.read_csv(in_csv0, low_memory=False)
    prev_df = prep_cases_df(prev_df)

    new_cols = list(set(curr_df.columns) - set(prev_df.columns))
    if not all([col_name in CASE_SCHEMA.keys() for col_name in new_cols]):
        print("New columns found, please update")
        mongo_client.close()
        sys.exit()

    # if len(new_cols) > 0:
    #     print("Added new columns")
    #     defaults = {
    #         k: v["default"]
    #         for col_name in new_cols
    #         for k, v in CASE_SCHEMA.items()
    #         if k == col_name
    #     }
    #     mongo_col.update_many({}, {"$set": defaults})

    curr_cnt = mongo_col.aggregate(
        [{"$group": {"_id": 1, "count": {"$sum": 1}}}]
    ).next()["count"]
    print("Current count: {}".format(curr_cnt))

    common_cols = list(set(prev_df.columns) & set(curr_df.columns))
    new_df = pd.concat(
        [prev_df[common_cols], prev_df[common_cols], curr_df[common_cols]]
    ).drop_duplicates(keep=False)
    new_df = curr_df.loc[curr_df["caseCode"].isin(new_df["caseCode"])].copy()

    new_df = make_mappable(new_df)

    # region deleted entries
    del_case_code = list(set(prev_df["caseCode"]) - set(curr_df["caseCode"]))
    del_df = pd.DataFrame(
        list(mongo_col.find({"caseCode": {"$in": del_case_code}}))
    ).drop(columns=["_id"], errors="ignore")
    if del_df.shape[0] > 0:
        mongo_col.delete_many({"caseCode": {"$in": del_case_code}})
        del_df["deletedAt"] = new_date
        for col_name in del_df.select_dtypes(include=["datetime64"]).columns:
            del_df[col_name] = del_df[col_name].fillna(0)
        data_dict = del_df.to_dict("records")
        mongo_col_del = mongo_db["cases.deleted"]
        mongo_col_del.insert_many(data_dict)
        print("Deleted entries: {}".format(len(del_case_code)))
    # endregion deleted entries

    # region updated entries
    exist_df = pd.DataFrame(
        mongo_col.find(
            {"caseCode": {"$in": new_df["caseCode"].to_list()}},
            {"caseCode": 1, "createdAt": 1},
        )
    )

    update_df = exist_df.merge(new_df, on=["caseCode"])
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
        new_df = new_df.loc[~new_df["caseCode"].isin(update_df["caseCode"])].copy()

    if new_df.shape[0] > 0:
        new_df["createdAt"] = new_date
        data_dict = new_df.to_dict("records")
        mongo_col.insert_many(data_dict)
        print("New entries: {}".format(new_df.shape[0]))
    # endregion new entries

    new_cnt = mongo_col.aggregate(
        [{"$group": {"_id": 1, "count": {"$sum": 1}}}]
    ).next()["count"]
    print("New count: {}".format(new_cnt))

    mongo_client.close()


if __name__ == "__main__":
    main()
