import os

from dotenv import load_dotenv

# load_dotenv(os.path.join(os.environ["USERPROFILE"], ".env"))
load_dotenv(os.path.join(os.getcwd(), ".env"))

assert os.environ["MONGODB_URI"], "MONGODB_URI not set"
assert os.environ["S3_ACCESS_KEY"], "S3_ACCESS_KEY not set"
assert os.environ["S3_SECRET_KEY"], "S3_SECRET_KEY not set"
assert os.environ["S3_URL"], "S3_URL not set"
assert os.environ["S3_BUCKET"], "S3_BUCKET not set"
assert os.getenv("REDIS_URL", None) is not None, "REDIS_URL is not set"
assert os.getenv("REDIS_PASSWORD", None) is not None, "REDIS_PASSWORD is not set"

# create variables for all environment variables

MONGODB_URI = os.environ["MONGODB_URI"]
ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_URL = os.environ["S3_URL"]
S3_BUCKET = os.environ["S3_BUCKET"]

REDIS_URL, REDIS_PORT = os.getenv("REDIS_URL").split(":")
REDIS_PASS = os.getenv("REDIS_PASSWORD")


# /create variables for all environment variables
