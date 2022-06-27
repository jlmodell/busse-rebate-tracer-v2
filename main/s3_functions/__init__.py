from s3_storage import CLIENT, S3_BUCKET
import json
from datetime import datetime
import os

from .savers import *
from .field_file import *


def get_field_file_body_and_decode_kwargs(prefix: str, key: str) -> dict:
    field_file = CLIENT.get_object(
        Bucket=S3_BUCKET, Key=prefix + key)['Body'].read().decode('utf-8')

    kwargs = json.loads(field_file)

    month = kwargs.get("month").upper()
    year = kwargs.get("year").upper()

    kwargs['period'] = f"{month}{year}-" + kwargs.get("period")

    return kwargs


def update_field_file_body_and_save(prefix: str, key: str, filter: dict, file_path: str, month: str, year: str) -> None:
    field_file = CLIENT.get_object(
        Bucket=S3_BUCKET, Key=prefix + key)['Body'].read().decode('utf-8')

    kwargs = json.loads(field_file)

    kwargs['filter'] = filter
    kwargs['file'] = os.path.basename(file_path)
    kwargs['month'] = month
    kwargs['year'] = year

    CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}backups/{datetime.now():%Y%m%d%H%M%S}_{key}",
        Body=json.dumps(kwargs),
        ContentType='application/json'
    )

    new_field_file = json.dumps(kwargs)

    CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=prefix + key,
        Body=new_field_file,
        ContentType='application/json'
    )


def save_df_to_s3_as_excel(df: pd.DataFrame, prefix: str, filename: str):
    data = GET_BYTES(df, filename)

    CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=prefix + filename,
        Body=data
    )


def save_tracings_df_as_html_with_javascript_css(df: pd.DataFrame, prefix: str, filename: str):
    data = GET_HTML_WITH_JS_CSS(df)

    CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=prefix + filename,
        Body=data
    )
