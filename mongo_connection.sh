#!/usr/bin/bash
echo $$
uri=$(cat .env | awk 'NR==1 {print $1}' | tr "\"" " " | awk '{print $2}' | tr "?" " " | awk '{print $1}')
eval "mongo $uri"
sleep 2
use "busserebatetraces"
