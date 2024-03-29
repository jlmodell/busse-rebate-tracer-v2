import json
import os
import re
import sys

import pandas as pd
from constants import DATA_WAREHOUSE, TRACINGS
from database import (
    delete_documents,
    gc_rbt,
    get_documents,
    insert_documents,
    push_to_redis_queue,
)
from finders import find_tracings_and_save
from s3_functions import (
    save_df_to_s3_as_excel,
    save_tracings_df_as_html_with_javascript_css,
    update_field_file_body_and_save,
)
from transformers import (
    GET_CLEAN_DF_TO_INGEST,
    build_df_from_df_fields_file,
    build_df_from_warehouse_using_fields_file,
    get_field_file_body_and_decode_kwargs,
    ingest_concordance_data_files,
    ingest_to_data_warehouse,
)

# from constants import *
# from database import *
# from finders import *
# from rich import print
# from s3_functions import *
# from s3_storage import *
# from transformers import *


def main():

    file_path = r"/mnt/c/temp/concordance_we_230120.xlsx"

    options = dict(
        file_path=file_path,
        year="2023",
        month="01",
        overwrite=True,
        delimiter=",",
        header_row=0,
        fields_file="concordance.json",
    )

    dtype = dict(
        pd.read_excel(
            options["file_path"],
            header=options["header_row"] if options["header_row"] != -1 else None,
        ).dtypes
    )

    try:
        for key in dtype.keys():
            if re.search(r"(PART|CAT|MATERIAL|ITEM)", key, re.IGNORECASE):
                dtype[key] = "str"
    except Exception as e:
        print(e)

    df = pd.read_excel(options["file_path"], dtype=dtype, header=options["header_row"])

    df = GET_CLEAN_DF_TO_INGEST(
        df=df,
        file_path=options["file_path"],
        month=options["month"],
        year=options["year"],
    )

    df = build_df_from_df_fields_file(df, options["fields_file"])

    # convert df to json
    result = df.to_json(orient="table", index=False)

    parsed = json.loads(result)["data"]

    for row in parsed:
        push_to_redis_queue(row)

    return df, parsed


if __name__ == "__main__":

    if len(sys.argv) <= 1:
        #         print(
        #             """
        #     Usage:

        #     --ingest_file --file_path=<file_path> --year=<year:yyyy> --month=<month:mm> --overwrite=<true|false> Optional [--delimiter=<delimiter> --header_row=<-1,0,1>]
        #     --ingest_folder --folder_path=<folder_path> --year=<year:yyyy> --month=<month:mm> --overwrite=<true|false>

        #     --read_fields_file --fields_file=<fields_file:string>
        #     --update_fields_file --fields_file=<fields_file:string> --month=<month:string> --year=<year:string> _
        #         --filter=<__file__:string;__month__:string;__year__:string> --overwrite=<true|false>

        #         eg) '{\\"__file__\\":\\"NDC_Rebate_Sales_Trace_202205.xlsx\\",\\"__month__\\":\\"05\\",\\"__year__\\":\\"2022\\"}'
        #         eg) '{\\"__file__\\":\\"^concordance(?!.*mms).*220610.xls$\\",\\"__month__\\":\\"05\\",\\"__year__\\":\\"2022\\"}'

        #     --update_tracing_data --fields_file=<fields_file> --overwrite=<true|false>

        #     --find_tracings_by_period --month=<month:string> --year=<year:string> --overwrite=<true|false>

        #     --delete_data_warehouse_by_filter --filter=<__file__:string;__month__:string;__year__:string> --overwrite=<true|false>

        #         eg) '{\\"__file__\\":\\"NDC_Rebate_Sales_Trace_202205.xlsx\\",\\"__month__\\":\\"05\\",\\"__year__\\":\\"2022\\"}'
        #         eg) '{\\"__file__\\":\\"^concordance(?!.*mms).*220610.xls$\\",\\"__month__\\":\\"05\\",\\"__year__\\":\\"2022\\"}'

        #     python main.py --debug
        #     python main.py --test
        # """
        #         )

        df, result = main()

    if len(sys.argv) > 1:
        if sys.argv[1] == "--help":
            print(
                """
            Usage:
            python main.py
            """
            )
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

            print(
                get_field_file_body_and_decode_kwargs("input/", options["fields_file"])
            )

        elif sys.argv[1] == "--update_fields_file":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            assert "fields_file" in options, "fields_file is required"
            assert "filter" in options, "filter is required"

            print(options["filter"])

            options["filter"] = json.loads(options["filter"])

            # assert "file_path" in options, "file_path is required"
            assert "month" in options, "month is required"
            assert "year" in options, "year is required"

            try:
                if options["overwrite"]:
                    options["overwrite"] = (
                        True if options["overwrite"].lower().startswith("t") else False
                    )
            except KeyError:
                options["overwrite"] = False
                print(
                    "\n\tOverwrite: \t> ",
                    options["overwrite"],
                    "\t> ",
                    "set `--overwrite=true` to overwrite\n",
                )

            if options["overwrite"]:
                print(f"Overwriting {options['fields_file']}")
                update_field_file_body_and_save(
                    "input/",
                    options["fields_file"],
                    options["filter"],
                    options["month"],
                    options["year"],
                    period=options.get("period", None),
                )

        elif sys.argv[1] == "--ingest_file":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            try:
                if options["overwrite"]:
                    options["overwrite"] = (
                        True if options["overwrite"].lower().startswith("t") else False
                    )
            except KeyError:
                options["overwrite"] = False
                print(
                    "\n\tOverwrite: \t> ",
                    options["overwrite"],
                    "\t> ",
                    "set `--overwrite=true` to overwrite\n",
                )

            assert "file_path" in options, "--file_path is required"
            assert "year" in options, "--year is required"
            assert "month" in options, "--month is required"

            ingest_to_data_warehouse(
                file_path=options["file_path"],
                year=options["year"],
                month=options["month"],
                overwrite=options["overwrite"],
                delimiter=options.get("delimiter", ","),
                header_row=int(options.get("header_row", 0)),
            )

        elif sys.argv[1] == "--ingest_file_s3":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            try:
                if options["overwrite"]:
                    options["overwrite"] = (
                        True if options["overwrite"].lower().startswith("t") else False
                    )
            except KeyError:
                options["overwrite"] = False
                print(
                    "\n\tOverwrite: \t> ",
                    options["overwrite"],
                    "\t> ",
                    "set `--overwrite=true` to overwrite\n",
                )

            assert "prefix" in options, "--prefix is required"
            assert "key" in options, "--key is required"
            assert "year" in options, "--year is required"
            assert "month" in options, "--month is required"

            ingest_to_data_warehouse(
                file_path=None,
                prefix=options["prefix"],
                key=options["key"],
                year=options["year"],
                month=options["month"],
                overwrite=options["overwrite"],
                delimiter=options.get("delimiter", ","),
                header_row=int(options.get("header_row", 0)),
            )

        elif sys.argv[1] == "--ingest_folder":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            try:
                if options["overwrite"]:
                    options["overwrite"] = (
                        True if options["overwrite"].lower().startswith("t") else False
                    )
            except KeyError:
                options["overwrite"] = False
                print(
                    "\n\tOverwrite: \t> ",
                    options["overwrite"],
                    "\t> ",
                    "set `--overwrite=true` to overwrite\n",
                )

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
                    options["overwrite"] = (
                        True if options["overwrite"].lower().startswith("t") else False
                    )
            except KeyError:
                options["overwrite"] = False
                print(
                    "\n\tOverwrite: \t> ",
                    options["overwrite"],
                    "\t> ",
                    "set `--overwrite=true` to overwrite\n",
                )

            assert "fields_file" in options, "fields_file is required"

            fields_file = get_field_file_body_and_decode_kwargs(
                "input/", options["fields_file"]
            )

            print(fields_file.get("period"))

            df = build_df_from_warehouse_using_fields_file(
                fields_file=options["fields_file"]
            )

            print(df)

            if options["overwrite"]:
                collection = gc_rbt(TRACINGS)

                delete_documents(collection, {"period": fields_file.get("period")})

                insert_documents(collection, df.to_dict(orient="records"))

        elif sys.argv[1] == "--find_tracings_by_period":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            assert "month" in options, "month is required"
            assert "year" in options, "year is required"

            try:
                if options["overwrite"]:
                    options["overwrite"] = (
                        True if options["overwrite"].lower().startswith("t") else False
                    )
            except KeyError:
                options["overwrite"] = False
                print(
                    "\n\tOverwrite: \t> ",
                    options["overwrite"],
                    "\t> ",
                    "set `--overwrite=true` to overwrite\n",
                )

            df = find_tracings_and_save(
                month=options["month"],
                year=options["year"],
                overwrite=options["overwrite"],
            )

            if options["overwrite"]:
                print(f"Overwriting tracings at s3:output\\")
            else:
                print(df)

        elif sys.argv[1] == "--delete_data_warehouse_by_filter":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            assert "filter" in options, "filter is required"

            print(options["filter"])

            options["filter"] = json.loads(options["filter"])

            try:
                if options["overwrite"]:
                    options["overwrite"] = (
                        True if options["overwrite"].lower().startswith("t") else False
                    )
            except KeyError:
                options["overwrite"] = False
                print(
                    "\n\tOverwrite: \t> ",
                    options["overwrite"],
                    "\t> ",
                    "set `--overwrite=true` to overwrite\n",
                )

            collection = gc_rbt(DATA_WAREHOUSE)

            if options["overwrite"]:
                delete_documents(collection, options["filter"])
            else:
                print(get_documents(collection, options["filter"]))

        elif sys.argv[1] == "--clean_up_mohawk_csv":
            options = {}
            for arg in sys.argv[2:]:
                if arg.startswith("--"):
                    key, value = arg.lstrip("--").split("=")
                    options[key] = value

            assert "file_path" in options, "file_path is required"

            with open(options.get("file_path", None), "r") as f:
                lines = f.readlines()

            with open(options.get("file_path", None), "w") as f:
                for line in lines:
                    line.replace('="', "")
                    line.replace('"', "")
                    f.write(line)

        elif sys.argv[1] == "--test":
            df = pd.DataFrame(["hello", "world"])
            prefix = "output/"

            # 0
            print(gc_rbt("tracings"))
            # 1
            print(get_field_file_body_and_decode_kwargs("input/", "cardinal.json"))
            # 2
            save_df_to_s3_as_excel(df, prefix, "test.xlsm")
            # 3
            save_tracings_df_as_html_with_javascript_css(df, prefix, "test.html")
            # 4
            file_path = os.path.join(
                os.environ["USERPROFILE"], "Downloads", "MGM_06112022_091341.xlsx"
            )
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
