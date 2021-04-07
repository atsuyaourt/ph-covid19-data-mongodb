import sys
import json
from pathlib import Path
from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup

import pandas as pd
import geopandas as gpd

from constants import MONGO_DB_URL


def fetch_psgc(url):
    pager = [1]
    i = 1
    out_df = []
    while len(pager) > 0:
        print(f"Getting data from page {i}")
        pg = requests.get(f"{url}&page={i}")
        soup = BeautifulSoup(pg.text, "html.parser")
        # table = soup.find_all('table')[0]
        for table in soup.find_all("table"):
            rows = [
                [c.get_text() for c in r.find_all(["th", "td"])]
                for r in table.find_all("tr")
            ]
            out_df.append(pd.DataFrame(rows[1:], columns=rows[0]))
            i = i + 1
        pager = soup.find_all("ul", class_="pager")
        if len(pager):
            pager = pager[0].find_all("li", class_="last")
        else:
            pager = []
    out_df = pd.concat(out_df)
    out_df.columns = out_df.columns.str.lower().str.replace("[\W]", "", regex=True)
    out_df.rename(columns={"population2015census": "population2015"}, inplace=True)
    out_df["population2015"] = (
        out_df["population2015"]
        .str.replace("[^0-9]", "", regex=True)
        .astype(int, errors="ignore")
    )
    out_df["incomeclass"] = (
        out_df["incomeclass"]
        .str.replace("[^0-9]", "", regex=True)
        .astype(int, errors="ignore")
    )
    return out_df


in_gdf = gpd.read_file(Path("input/geojson/ph-prov.geojson"))
in_gdf.columns = in_gdf.columns.str.lower()
in_gdf.rename(
    columns={
        "adm2_en": "adm2",
        "adm2_pcode": "adm2Pcode",
        "adm1_en": "adm1",
        "adm1_pcode": "adm1Pcode",
        "adm_id": "admId",
        "updated": "updatedAt",
    },
    inplace=True,
)

print("Fetching Province info...")
url = "https://psa.gov.ph/classification/psgc/?q=psgc/provinces"
in_df = fetch_psgc(url)
in_df["code"] = "PH" + in_df["code"]
in_df.rename(columns={"province": "name"}, inplace=True)
in_df = pd.concat(
    [
        in_df,
        pd.DataFrame(
            [
                {
                    "name": "NCR, 1ST DISTRICT",
                    "code": "PH133900000",
                    "population2015": 1780148,
                },
                {
                    "name": "NCR, 2ND DISTRICT",
                    "code": "PH137400000",
                    "population2015": 4650613,
                },
                {
                    "name": "NCR, 3RD DISTRICT",
                    "code": "PH137500000",
                    "population2015": 2819388,
                },
                {
                    "name": "NCR, 4TH DISTRICT",
                    "code": "PH137600000",
                    "population2015": 3627104,
                },
            ]
        ),
    ]
)

out_gdf = in_gdf.merge(
    in_df[["code", "incomeclass", "population2015"]],
    how="left",
    left_on="adm2Pcode",
    right_on="code",
).drop(columns=["code"])

out_file = Path("output/geojson/ph-prov.geojson")
out_file.parent.mkdir(parents=True, exist_ok=True)
out_gdf.to_file("output/geojson/ph-prov.geojson", driver="GeoJSON")

# region mongodb
print("Connecting to mongodb...")
mongo_client = MongoClient(MONGO_DB_URL)
mongo_db = mongo_client["defaultDb"]
mongo_col = mongo_db["geomaps"]
print("Connection successful...")
# endregion mongodb

mongo_col.delete_one({"name": "ph-prov"})
with open(out_file) as file:
    geo_data = json.load(file)
data_dict = dict(name="ph-prov", geo=geo_data)
mongo_col.insert_one(data_dict)
