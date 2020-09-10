import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

import pandas as pd

from helpers import prep_data

load_dotenv()
# load_dotenv(dotenv_path=Path("..")/".env")

mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
mongo_db = mongo_client["default"]
mongo_col = mongo_db["case_info"]

in_csv = list(Path("input/csv").glob("*case_info.csv"))[-1]
curr_df = pd.read_csv(in_csv)
curr_df = prep_data(curr_df)
date_str = in_csv.name.split("_")[0]
new_date = pd.to_datetime(date_str).tz_localize("Asia/Manila")
print("Date: {}".format(new_date))

in_csv0 = list(Path("input/csv").glob("*case_info.csv"))[-2]
prev_df = pd.read_csv(in_csv0)
prev_df = prep_data(prev_df)

curr_cnt = len(mongo_col.distinct("caseCode", {"healthStatus": {"$not": {"$eq": "invalid"}}}))
print("Current count: {}".format(curr_cnt))

# region updated entrie
exist_df = pd.DataFrame(
    mongo_col.find(
        {"caseCode": {"$in": curr_df["caseCode"].to_list()}, "healthStatus": {"$not": {"$eq": "invalid"}}},
        {"caseCode": 1, "healthStatus": 1, "createdAt": 1},
    )
)

update_df = exist_df.merge(curr_df, on=["caseCode", "healthStatus"])
update_ids = update_df["_id"].to_list()

if update_df.shape[0] > 0:
    mongo_col.delete_many({"_id": {"$in": update_ids}})
    _update_df = update_df.drop(columns=["_id"])
    _update_df["updatedAt"] = new_date
    data_dict = _update_df.to_dict("records")
    mongo_col.insert_many(data_dict)
    print("Updated entries: {}".format(update_df.shape[0]))
# endregion updated entries

# region new entries
__update_df = update_df.drop(columns=["_id", "createdAt"])
new_df = pd.concat([__update_df, __update_df, curr_df]).drop_duplicates(subset=["caseCode", "healthStatus"], keep=False)
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
    print("Deleted entries: {}".format(del_case_code))

new_cnt = len(mongo_col.distinct("caseCode", {"healthStatus": {"$not": {"$eq": "invalid"}}}))
print("New CSV count: {}".format(curr_df.shape[0]))
print("New DB count: {}".format(new_cnt))

mongo_client.close()
