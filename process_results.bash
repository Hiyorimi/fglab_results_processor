#!/bin/bash
echo "bash script started $1 - $2 - $3"

param_1=$1
param_2=$2 
param_3=$3
dirname="${param_1}_${param2}_${param3}"

sleep 5
mkdir -p ./examples/$dirname
echo "emulated delay and results processing"
