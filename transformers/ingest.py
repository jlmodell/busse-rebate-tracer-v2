import re
import os
import pandas as pd
import numpy as np


def GET_DTYPES(file_path: str, delimiter: str = ",") -> dict:
    if re.search(r".xl(sx|sm|s)$", file_path, re.I):
        dtypes = dict(pd.read_excel(file_path).dtypes)
    elif re.search(r".(csv|txt)$", file_path, re.I):
        dtypes = dict(pd.read_csv(file_path, delimiter=delimiter).dtypes)

    for key in dtypes.keys():
        if re.search(r"(PART|CAT|MATERIAL|ITEM)", key, re.IGNORECASE):
            dtypes[key] = "str"

    return dtypes


def GET_CLEAN_DF_TO_INGEST(df: pd.DataFrame, file_path: str, month: str, year: str) -> pd.DataFrame:
    df.fillna("")
    df = df[df[df.columns[0]].notnull()].copy()
    df = df[df[df.columns[-1]].notnull()].copy()
    df = df[df[df.columns[0]] != ""].copy()

    df = df.replace(np.nan, "", regex=True).copy()

    df["__file__"] = os.path.basename(file_path)
    df["__year__"] = year
    df["__month__"] = month

    df.columns = [str(x) for x in df.columns]

    return df
