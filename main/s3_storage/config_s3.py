from telnetlib import SE
import boto3
import s3fs

from constants import ACCESS_KEY, SECRET_KEY, S3_URL, S3_BUCKET

from rich import print

# print(ACCESS_KEY, SECRET_KEY, S3_URL, S3_BUCKET, f'endpoint_url": "{S3_URL}"')

# S3_S3FS = s3fs.S3FileSystem(
#     key=ACCESS_KEY,
#     secret=SECRET_KEY,
#     client_kwargs={f'endpoint_url": "{S3_URL}"'},
# )

RESOURCE = boto3.resource(
    "s3",
    endpoint_url=S3_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

CLIENT = boto3.client(
    "s3",
    endpoint_url=S3_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

BUCKET = RESOURCE.Bucket(S3_BUCKET)
