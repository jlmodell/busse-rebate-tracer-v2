#!/bin/sh
PWD=`pwd`
echo $PWD
activate() {
    . $PWD/bin/activate
}
activate

python main --ingest_folder --folder_path=/mnt/c/Users/jmodell.BUSSEINC0/Downloads/concordance_we_$1 --month=$2 --year=$3 --overwrite=true
python main --update_tracing_data --fields_file=concordance.json --overwrite=true
python main --update_tracing_data --fields_file=concordance_mms.json --overwrite=true

