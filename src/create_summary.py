import os
import sys
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

import pandas as pd

from helpers import prep_data
from models import CASE_SCHEMA, REGION_MAP, REGION_UNKNOWN
from make_mappable import update_loc_city_mun, update_loc_province

load_dotenv()


def main():
    in_csvs = list(Path("input/csv").glob("*case_info.csv"))
    in_csvs.sort()
    in_csv = in_csvs[-1]
    curr_df = pd.read_csv(in_csv)
    curr_df = prep_data(curr_df)

    date_str = in_csv.name.split("_")[0]
    new_date = pd.to_datetime(date_str).tz_localize("Asia/Manila")
    print("Date: {}".format(new_date))

    # region with cityMun
    curr_df.loc[:, "regionResGeo"] = curr_df["regionRes"].map(REGION_MAP)
    with_city_mun_df = curr_df.loc[
        (~((curr_df["cityMunRes"] == "") | (curr_df["cityMunRes"].isna())))
        & (~(curr_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_city_mun_idx = with_city_mun_df.index.to_list()

    with_city_mun_df = update_loc_city_mun(with_city_mun_df)
    # endregion with cityMun

    # region with province
    with_prov_df = curr_df.loc[
        (~(curr_df.index.isin(with_city_mun_idx)))
        & (~((curr_df["provRes"] == "") | (curr_df["provRes"].isna())))
        & (~(curr_df["regionResGeo"].isin(REGION_UNKNOWN)))
    ].copy()
    with_prov_idx = with_prov_df.index.to_list()

    with_prov_df = update_loc_province(with_prov_df)
    # endregion with province


    stats_df = pd.concat(
        [
            with_city_mun_df,
            with_prov_df,
            curr_df.loc[
                ~(curr_df.index.isin(with_city_mun_idx + with_prov_idx))
            ].copy(),
        ]
    )
    stats_df = (
        stats_df.groupby(
            ["regionResGeo", "provResGeo", "sex", "age", "healthStatus"]
        )["caseCode"]
        .nunique()
        .reset_index()
    )
    stats_df["createdAt"] = new_date
    stats_df = stats_df.rename(columns={"caseCode": "count"})

    # region mongodb
    print("Connecting to mongodb...")
    mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
    if "default" not in mongo_client.list_database_names():
        print("Database 'default' not found... exiting...")
        mongo_client.close()
        sys.exit()
    print("using 'default' database.")
    mongo_db = mongo_client["default"]

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

    mongo_client.close()
    print("Connection closed...")
    # endregion mongodb

    # region write to csv
    print("Creating a csv...")
    stats_df.to_csv(Path(f"output/summary/case_stats_{date_str}.csv"), index=False)
    # end region write to csv


if __name__ == "__main__":
    main()
