#!/usr/bin/bash
wasabi=https://s3.wasabisys.com/
echo "Input JSON Files"
aws s3 --endpoint-url=$wasabi ls s3://busserebatetracing/input/
echo "--------------------------------------------------------------------------"

if [ -n "$1" ]
then
   echo "'$1.json' File - Config"
   aws s3 --endpoint-url=$wasabi cp s3://busserebatetracing/input/$1.json - | dasel select -r json
fi

echo "--------------------------------------------------------------------------"
echo "update command reminder..."
echo "cmd = aws s3 --endpoint-url=https://s3.wasabisys.com/ cp s3://busserebatetracing/input/<file>.json - | dasel put string -r json '.<field>' '<value'"
echo "--------------------------------------------------------------------------"
