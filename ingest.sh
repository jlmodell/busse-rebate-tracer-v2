#!/bin/sh
PWD=`pwd`
activate() {
    . $PWD/bin/activate
}
activate

python main --ingest_file --file_path=/mnt/c/Users/jmodell.BUSSEINC0/Downloads/$1 --month=$2 --year=$3 --overwrite=true
python main --update_tracing_data --fields_file=$4 --overwrite=true

