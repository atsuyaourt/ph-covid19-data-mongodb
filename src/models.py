from numpy import nan
from pandas import to_datetime

from helpers import camel_case

SEX_ENUM = ["male", "female"]
REMOVAL_TYPE_ENUM = ["recovered", "died"]
HEALTH_STATS_ENUM = [
    "recovered",
    "died",
    "confirmed",
    "asymptomatic",
    "mild",
    "moderate",
    "severe",
    "critical",
]

CASE_SCHEMA = dict(
    caseCode=dict(dtype="String"),
    age=dict(dtype="Integer"),
    sex=dict(dtype="String", choices=SEX_ENUM),
    dateSpecimen=dict(dtype="Date"),
    dateResultRelease=dict(dtype="Date"),
    dateRepConf=dict(dtype="Date"),
    dateDied=dict(dtype="Date"),
    dateRecover=dict(dtype="Date"),
    dateRepRem=dict(dtype="Date"),
    dateOnset=dict(dtype="Date"),
    healthStatus=dict(dtype="String", choices=HEALTH_STATS_ENUM),
    removalType=dict(dtype="String", choices=REMOVAL_TYPE_ENUM),
    regionRes=dict(dtype="String"),
    provRes=dict(dtype="String"),
    cityMunRes=dict(dtype="String"),
    cityMuniPSGC=dict(dtype="String"),
    barangayRes=dict(dtype="String"),
    barangayPSGC=dict(dtype="String"),
    isAdmitted=dict(dtype="Bool"),
    isQuarantined=dict(dtype="Bool"),
    isPregnant=dict(dtype="Bool"),
)

CASE_FIELD_MAP = dict(
    Admitted="isAdmitted", Quarantined="isQuarantined", Pregnanttab="isPregnant"
)
CASE_FIELD_DROP = ["ageGroup", "validationStatus"]

REGION_MAP = {
    "Region I: Ilocos Region": "Region I",
    "Region II: Cagayan Valley": "Region II",
    "Region III: Central Luzon": "Region III",
    "Region IV-A: CALABARZON": "Region IV-A",
    "Region IV-B: MIMAROPA": "Region IV-B",
    "Region V: Bicol Region": "Region V",
    "Region VI: Western Visayas": "Region VI",
    "Region VII: Central Visayas": "Region VII",
    "Region VIII: Eastern Visayas": "Region VIII",
    "Region IX: Zamboanga Peninsula": "Region IX",
    "Region X: Northern Mindanao": "Region X",
    "Region XI: Davao Region": "Region XI",
    "Region XII: SOCCSKSARGEN": "Region XII",
    "CARAGA": "Region XIII",
    "CAR": "Cordillera Administrative Region",
    "NCR": "National Capital Region",
    "4B": "Region IV-B",
    "MIMAROPA": "Region IV-B",
    "BARMM": "Bangsamoro Autonomous Region in Muslim Mindanao",
    "REGION I (ILOCOS REGION)": "Region I",
    "REGION II (CAGAYAN VALLEY)": "Region II",
    "REGION III (CENTRAL LUZON)": "Region III",
    "REGION IV-A (CALABAR ZON)": "Region IV-A",
    "REGION IV-B (MIMAROPA)": "Region IV-B",
    "REGION V (BICOL REGION)": "Region V",
    "REGION VI (WESTERN VISAYAS)": "Region VI",
    "REGION VII (CENTRAL VISAYAS)": "Region VII",
    "REGION VIII (EASTERN VISAYAS)": "Region VIII",
    "REGION IX (ZAMBOANGA PENINSULA)": "Region IX",
    "REGION X (NORTHERN MINDANAO)": "Region X",
    "REGION XI (DAVAO REGION)": "Region XI",
    "REGION XII (SOCCSKSA RGEN)": "Region XII",
    "REGION XIII (CARAGA)": "Region XIII",
    "AUTONOMOUS REGION IN MUSLIM MINDANAO (ARMM)": "Bangsamoro Autonomous Region in Muslim Mindanao",
    "CORDILLERA ADMINISTRA TIVE REGION (CAR)": "Cordillera Administrative Region",
    "NATIONAL CAPITAL REGION (NCR)": "National Capital Region",
    "CAR": "Cordillera Administrative Region",
    "REGION 1": "Region I",
    "REGION 2": "Region II",
    "REGION 3": "Region III",
    "REGION 4-A": "Region IV-A",
    "REGION 4-B": "Region IV-B",
    "REGION 5": "Region V",
    "REGION 6": "Region VI",
    "REGION 7": "Region VII",
    "REGION 8": "Region VIII",
    "REGION 9": "Region IX",
    "REGION 10": "Region X",
    "REGION 11": "Region XI",
    "REGION 12": "Region XII",
    "CARAGA": "Region XIII",
    "ARMM": "Bangsamoro Autonomous Region in Muslim Mindanao",
    "REPATRIATE": "Repatriate",
    "Repatriate": "Repatriate",
    "ROF": "ROF",
    "": "",
}

REGION_UNKNOWN = ["ROF", "Repatriate", ""]


def prep_cases_df(df):
    df = df.rename(columns=CASE_FIELD_MAP)
    df.columns = [camel_case(c) for c in df.columns]
    df = df.drop(columns=CASE_FIELD_DROP, errors="ignore")
    valid = True
    for col_name in df.columns:
        if col_name in CASE_SCHEMA:
            if CASE_SCHEMA[col_name]["dtype"] == "String":
                if "choices" in CASE_SCHEMA[col_name]:
                    df[col_name] = df[col_name].str.strip().str.lower()
                    no_match = (
                        df.dropna()
                        .loc[
                            ~df[col_name].isin(CASE_SCHEMA[col_name]["choices"]),
                            col_name,
                        ]
                        .unique()
                    )
                    if len(no_match) > 0:
                        print(
                            f"{col_name} has unmatched values: {no_match}!!! exiting..."
                        )
                        valid = False
                        break
                if "default" in CASE_SCHEMA[col_name]:
                    df.loc[df[col_name].isna(), col_name] = CASE_SCHEMA[col_name][
                        "default"
                    ]
                else:
                    df.loc[df[col_name].isna(), col_name] = ""
                df[col_name] = df[col_name].str.strip()
            elif CASE_SCHEMA[col_name]["dtype"] == "Integer":
                if "default" in CASE_SCHEMA[col_name]:
                    df.loc[df[col_name].isna(), col_name] = CASE_SCHEMA[col_name][
                        "default"
                    ]
                else:
                    df.loc[df[col_name].isna(), col_name] = -1
                df[col_name] = df[col_name].map(int, na_action="ignore")
            elif CASE_SCHEMA[col_name]["dtype"] == "Bool":
                df[col_name] = (
                    df[col_name].str.strip().str.lower().map({"yes": True, "no": False})
                )
            elif CASE_SCHEMA[col_name]["dtype"] == "Date":
                df[col_name] = to_datetime(df[col_name]).dt.tz_localize("Asia/Manila")
                if "default" in CASE_SCHEMA[col_name]:
                    df.loc[df[col_name].isna(), col_name] = CASE_SCHEMA[col_name][
                        "default"
                    ]
                else:
                    df.loc[df[col_name].isna(), col_name] = 0
        else:
            print("Warning: Don't know how to format column: {}".format(col_name))
            df.loc[df[col_name].isna(), col_name] = ""
            df[col_name] = df[col_name].str.strip()
    return df
