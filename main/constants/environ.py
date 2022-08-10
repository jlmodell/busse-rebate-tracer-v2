from dotenv import load_dotenv
import os

# load_dotenv(os.path.join(os.environ["USERPROFILE"], ".env"))
load_dotenv(os.path.join(os.getcwd(), '.env'))

assert os.environ["MONGODB_URI"], "MONGODB_URI not set"
assert os.environ["S3_ACCESS_KEY"], "S3_ACCESS_KEY not set"
assert os.environ["S3_SECRET_KEY"], "S3_SECRET_KEY not set"
assert os.environ["S3_URL"], "S3_URL not set"
assert os.environ["S3_BUCKET"], "S3_BUCKET not set"

# create variables for all environment variables

MONGODB_URI = os.environ["MONGODB_URI"]
ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_URL = os.environ["S3_URL"]
S3_BUCKET = os.environ["S3_BUCKET"]

# /create variables for all environment variables
