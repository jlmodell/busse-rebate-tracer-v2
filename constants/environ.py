from rich import print
from dotenv import load_dotenv
import os

load_dotenv()
# load_dotenv(os.path.join(os.getcwd(), '.env'))

assert os.environ["MONGODB_URI"], "MONGODB_URI not set"

MONGODB_URI = os.environ["MONGODB_URI"]
