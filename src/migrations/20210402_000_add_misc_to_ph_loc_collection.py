import os
import sys
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

import pandas as pd
import geopandas as gpd

load_dotenv()


def main():
    # region mongodb
    print("Connecting to mongodb...")
    mongo_client = MongoClient(os.getenv("MONGO_DB_URL"))
    mongo_db = mongo_client["defaultDb"]
    mongo_col = mongo_db["ph_loc"]
    print("Connection successful...")
    # endregion mongodbS

    # insert data
    # data_dict = out_df.to_dict("records")
    data_dict = [
        {"region": "ROF", "province": None, "muniCity": None, "type": "misc"},
        {"region": "Repatriate", "province": None, "muniCity": None, "type": "misc"},
        {"region": "", "province": None, "muniCity": None, "type": "misc"},
    ]
    mongo_col.insert_many(data_dict)

    loc_df = pd.DataFrame(
        list(mongo_col.find({"region": {"$in": ["ROF", "Repatriate", ""]}}))
    )

    mongo_col = mongo_db["cases"]
    for i, r in tqdm(loc_df.iterrows(), total=loc_df.shape[0]):
        mongo_col.update_many(
            {
                "regionRes": r["region"],
            },
            {"$set": {"locId": r["_id"]}},
        )


if __name__ == "__main__":
    main()
