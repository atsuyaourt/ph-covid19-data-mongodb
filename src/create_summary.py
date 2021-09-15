import sys
from dotenv import dotenv_values
from pymongo import MongoClient

import pandas as pd

from constants import CASE_INFO_CSV_DIR, TZ
from models import prep_cases_df
from make_mappable import make_mappable

config = dotenv_values()


def main():
    in_csvs = list(CASE_INFO_CSV_DIR.glob("*case_info.csv"))
    in_csvs.sort()
    in_csv = in_csvs[-1]
    curr_df = pd.read_csv(in_csv, low_memory=False)
    curr_df = prep_cases_df(curr_df)

    date_str = in_csv.name.split("_")[0]
    new_date = pd.to_datetime(date_str).tz_localize(TZ)
    print("Date: {}".format(new_date))

    _curr_df = curr_df.loc[
        curr_df.cityMuniPSGC.isna() | (curr_df.cityMuniPSGC == "")
    ].copy()
    _curr_df = make_mappable(_curr_df)
    _stats_df = _curr_df.loc[
        _curr_df.cityMuniPSGC.isna() | (_curr_df.cityMuniPSGC == "")
    ].copy()
    _curr_df = _curr_df.loc[~_curr_df.index.isin(_stats_df.index)].copy()
    stats_df = pd.concat(
        [
            curr_df.loc[
                ~(
                    curr_df.index.isin(
                        _curr_df.index.to_list() + _stats_df.index.to_list()
                    )
                )
            ],
            _curr_df,
        ]
    )
    stats_df["provincePSGC"] = stats_df.cityMuniPSGC.str.slice(0, 6) + "00000"

    stats_df = (
        stats_df.groupby(["provincePSGC", "healthStatus"])["caseCode"]
        .nunique()
        .reset_index()
    )
    activeHealthStats = ["asymptomatic", "mild", "moderate", "severe", "critical"]
    stats_active_df = (
        stats_df.loc[stats_df.healthStatus.isin(activeHealthStats)]
        .groupby(["provincePSGC"])
        .sum()
        .reset_index()
    )
    stats_active_df["healthStatus"] = "active"
    stats_all_df = stats_df.groupby(["provincePSGC"]).sum().reset_index()
    stats_all_df["healthStatus"] = "all"
    stats_df = pd.concat([stats_all_df, stats_active_df, stats_df])
    stats_df["createdAt"] = new_date
    stats_df = stats_df.rename(columns={"caseCode": "count"})

    # region mongodb
    print("Connecting to mongodb...")
    mongo_client = MongoClient(config["MONGO_DB_URL"])
    if "defaultDb" not in mongo_client.list_database_names():
        print("Database 'defaultDb' not found... exiting...")
        mongo_client.close()
        sys.exit()
    print("using 'defaultDb' database.")
    mongo_db = mongo_client["defaultDb"]

    print("using 'cases.stats' collectiom.")
    mongo_col = mongo_db["cases.stats"]
    if "cases.stats" in mongo_db.list_collection_names():
        # drop collection first
        print("Removing old data...")
        mongo_col.drop()

    print("Connection successful...")

    # add new data to database
    print("Adding new data...")
    data_dict = stats_df.to_dict("records")
    mongo_col.insert_many(data_dict)

    stats_df = curr_df.copy()

    stats_df = stats_df.groupby(["healthStatus"])["caseCode"].nunique().reset_index()
    activeHealthStats = ["asymptomatic", "mild", "moderate", "severe", "critical"]
    stats_active = (
        stats_df.loc[stats_df.healthStatus.isin(activeHealthStats)].sum().to_numpy()[1]
    )
    stats_all = stats_df.sum().to_numpy()[1]
    stats_df = stats_df.append(
        [
            {"healthStatus": "all", "caseCode": stats_all},
            {"healthStatus": "active", "caseCode": stats_active},
        ],
        ignore_index=True,
    )

    stats_df["createdAt"] = new_date
    stats_df = stats_df.rename(columns={"caseCode": "count"})

    print("using 'cases.summary' collectiom.")
    mongo_col = mongo_db["cases.summary"]
    if "cases.summary" in mongo_db.list_collection_names():
        # drop collection first
        print("Removing old data...")
        mongo_col.drop()

    print("Connection successful...")

    # add new data to database
    print("Adding new data...")
    data_dict = stats_df.to_dict("records")
    mongo_col.insert_many(data_dict)

    mongo_client.close()
    print("Connection closed...")
    # endregion mongodb


if __name__ == "__main__":
    main()
