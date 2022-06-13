import sys
import os
from rich import print

from constants import *
from database import *

from s3_storage import *
from s3_functions import *

from finders import *
from transformers import *

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
            sys.exit(0)

        elif sys.argv[1] == "--read_fields_file":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            assert "fields_file" in options, "fields_file is required"

            print(get_field_file_body_and_decode_kwargs(
                "input/", options["fields_file"]))

        elif sys.argv[1] == "--ingest_file":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            try:
                if options["overwrite"]:
                    options["overwrite"] = True if value.lower(
                    ).startswith("t") else False
            except KeyError:
                options["overwrite"] = False
                print("\n\tOverwrite: \t> ", options["overwrite"],
                      "\t> ", "set `--overwrite=true` to overwrite\n")

            assert "file_path" in options, "--file_path is required"
            assert "year" in options, "--year is required"
            assert "month" in options, "--month is required"

            ingest_to_data_warehouse(
                file_path=options["file_path"],
                year=options["year"],
                month=options["month"],
                overwrite=options["overwrite"],
            )

        elif sys.argv[1] == "--ingest_folder":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            try:
                if options["overwrite"]:
                    options["overwrite"] = True if value.lower(
                    ).startswith("t") else False
            except KeyError:
                options["overwrite"] = False
                print("\n\tOverwrite: \t> ", options["overwrite"],
                      "\t> ", "set `--overwrite=true` to overwrite\n")

            assert "folder_path" in options, "--folder_path is required"
            assert "year" in options, "--year is required"
            assert "month" in options, "--month is required"

            ingest_concordance_data_files(
                folder_path=options["folder_path"],
                year=options["year"],
                month=options["month"],
                overwrite=options["overwrite"],
            )

        elif sys.argv[1] == "--update_tracing_data":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            try:
                if options["overwrite"]:
                    options["overwrite"] = True if value.lower(
                    ).startswith("t") else False
            except KeyError:
                options["overwrite"] = False
                print("\n\tOverwrite: \t> ", options["overwrite"],
                      "\t> ", "set `--overwrite=true` to overwrite\n")

            assert "fields_file" in options, "fields_file is required"

            fields_file = get_field_file_body_and_decode_kwargs(
                "input/", options["fields_file"])

            print(fields_file.get("period"))

            df = build_df_from_warehouse_using_fields_file(
                fields_file=options["fields_file"])

            print(df)

            if options["overwrite"]:
                collection = gc_rbt(TRACINGS)

                delete_documents(collection, {
                    "period": fields_file.get("period")})

                insert_documents(collection, df.to_dict(orient="records"))

        elif sys.argv[1] == "--delete_by_filter":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            if options.get("filter", None) == None:
                options["filter"] = {
                    "__file__": {
                        "$regex": '^concordance(?!.*mms).*220610.xls$',
                        "$options": "i"
                    },
                    "__month__": "05",
                    "__year__": "2022",
                }

            collection = gc_rbt(DATA_WAREHOUSE)

            delete_documents(collection, options["filter"])

        elif sys.argv[1] == "--test":
            df = pd.DataFrame(["hello", "world"])
            prefix = "output/"

            # 0
            print(gc_rbt("tracings"))
            # 1
            print(get_field_file_body_and_decode_kwargs(
                "input/", "cardinal.json"))
            # 2
            save_df_to_s3_as_excel(df, prefix, "test.xlsm")
            # 3
            save_tracings_df_as_html_with_javascript_css(
                df, prefix, "test.html")
            # 4
            file_path = os.path.join(
                os.environ["USERPROFILE"], "Downloads", "MGM_06112022_091341.xlsx")
            ingest_to_data_warehouse(
                file_path=file_path,
                year="2022",
                month="06",
                overwrite=False,
            )
            # 5
            folder_path = os.path.join(
                os.environ["USERPROFILE"], "Downloads", "concordance_20220610"
            )
            ingest_concordance_data_files(
                folder_path=folder_path,
                year="2022",
                month="06",
                overwrite=False,
            )

    else:
        print("""
    Usage:

    --ingest_file --file_path=<file_path> --year=<year> --month=<month> --overwrite=<true|false>
    --ingest_folder --folder_path=<folder_path> --year=<year> --month=<month> --overwrite=<true|false>

    --read_fields_file --fields_file=<fields_file>

    --update_tracing_data --fields_file=<fields_file> --overwrite=<true|false>

    python main.py --debug
    python main.py --test
""")
