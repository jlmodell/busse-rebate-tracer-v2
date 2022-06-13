import sys
from rich import print

from constants.config import *
from constants.environ import MONGODB_URI

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help":
            print("""
            Usage:
            python main.py
            """)
            sys.exit(0)
        elif sys.argv[1] == "--debug":
            print(locals())
            print(globals())
    else:
        print("""
    Usage:
    python main.py --debug
""")
