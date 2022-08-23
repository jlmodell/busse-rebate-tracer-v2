#!/usr/bin/bash
for i in $@; do
  python main --update_tracing_data --fields_file=$i --overwrite=true
  echo $i
done
