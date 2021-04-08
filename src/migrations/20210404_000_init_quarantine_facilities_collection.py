import sys
from pathlib import Path
from pymongo import MongoClient

import pandas as pd

from constants import CASE_INFO_CSV_DIR, MONGO_DB_URL
from make_mappable import make_mappable


def main():
    # Load initial data
    in_csv = (
        CASE_INFO_CSV_DIR / "../quarantine_facility/20210403_quarantine_facility.csv"
    )
    if not in_csv.is_file():
        print("Error: Input file missing")
        sys.exit()
    in_df = pd.read_csv(in_csv, low_memory=False)
    in_df["isolbed"] = in_df["isolbed_o"] + in_df["isolbed_v"]
    in_df["beds_ward"] = in_df["beds_ward_o"] + in_df["beds_ward_v"]

    out_df = in_df.groupby(
        [
            "facilityname",
            "region",
            "province",
            "city_mun",
            "bgy",
            "region_psgc",
            "province_psgc",
            "city_mun_psgc",
            "bgy_psgc",
        ]
    )[["isolbed", "beds_ward"]].max(numeric_only=True)
    out_df = out_df.reset_index()
    out_df = out_df.rename(
        columns={"region": "regionRes", "province": "provRes", "city_mun": "cityMunRes"}
    )

    out_df = make_mappable(out_df)
    out_df = out_df.rename(
        columns={
            "facilityname": "facilityName",
            "regionRes": "region",
            "provRes": "province",
            "cityMunRes": "cityMun",
            "region_psgc": "regionPSGC",
            "province_psgc": "provincePSGC",
            "city_mun_psgc": "cityMuniPSGC",
            "bgy_psgc": "bgyPSGC",
            "isolbed": "isolBed",
            "beds_ward": "bedsWard",
        }
    )

    # region mongodb
    print("Connecting to mongodb...")
    mongo_client = MongoClient(MONGO_DB_URL)
    mongo_db = mongo_client["defaultDb"]
    mongo_col = mongo_db["quarantineFacilities"]
    if "quarantineFacilities" in mongo_db.list_collection_names():
        # drop collection first
        print("Removing old data...")
        mongo_col.drop()

    print("Connection successful...")
    # endregion mongodb

    # insert data
    data_dict = out_df.to_dict("records")
    mongo_col.insert_many(data_dict)


if __name__ == "__main__":
    main()
