from pandas import to_datetime

from vars import COL_MAP, COL_DTYPE


# function to convert string to camelCase
def camel_case(string):
    return string[0].lower() + string[1:]


def prep_data(df):
    df = df.rename(columns=COL_MAP)
    df.columns = [camel_case(c) for c in df.columns]
    for col_name in df.columns:
        if col_name in COL_DTYPE:
            if COL_DTYPE[col_name]["dtype"] == "String":
                df.loc[df[col_name].isna(), col_name] = ""
                df[col_name] = df[col_name].str.strip()
            elif COL_DTYPE[col_name]["dtype"] == "Integer":
                df.loc[df[col_name].isna(), col_name] = -1
                df[col_name] = df[col_name].map(int, na_action="ignore")
            elif COL_DTYPE[col_name]["dtype"] == "Bool":
                df[col_name] = df[col_name].str.strip().str.lower().map({"yes": True, "no": False})
            elif COL_DTYPE[col_name]["dtype"] == "Date":
                df.loc[df[col_name].isna(), col_name] = 0
                df[col_name] = to_datetime(df[col_name]).dt.tz_localize("Asia/Manila")
            elif COL_DTYPE[col_name]["dtype"] == "Enum":
                df[col_name] = df[col_name].str.strip().str.lower()
        else:
            print("Warning: Don't know how to format column: {}".format(col_name))
            df.loc[df[col_name].isna(), col_name] = ""
            df[col_name] = df[col_name].str.strip()
    return df
