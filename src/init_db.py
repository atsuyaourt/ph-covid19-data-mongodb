import os
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

import pandas as pd

from helpers import prep_data

load_dotenv()

# Load initial data
in_csv = Path("input/csv") / "20200609_case_info.csv"
if not in_csv.is_file():
    print("Error: Input file missing")
    exit
in_df = pd.read_csv(in_csv)
# prep data
in_df = prep_data(in_df)
in_df = in_df.drop(columns=["validationStatus"], errors="ignore")

date_str = in_csv.name.split("_")[0]
in_df["createdAt"] = pd.to_datetime(date_str).tz_localize("Asia/Manila")

# initialize monogodb
mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
mongo_db = mongo_client["default"]
mongo_col = mongo_db["cases"]

# insert data
data_dict = in_df.to_dict("records")
mongo_col.create_index(
    [
        ("createdAt", -1),
        ("caseCode", 1),
        ("healthStatus", 1),
    ],
    unique=True,
)
mongo_col.insert_many(data_dict)
