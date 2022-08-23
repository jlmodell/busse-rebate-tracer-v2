#!/bin/sh
PWD=`pwd`
activate() {
    . $PWD/bin/activate
}
activate

python main --find_tracings_by_period --month=$1 --year=$2 --overwrite=true
