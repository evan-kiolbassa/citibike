#!/bin/bash

if [[ $# -eq 1 ]] ; then
    echo 'usage:'
    echo './merge.sh pattern output.csv'
    exit 1
fi

output_file=$2

i=0

files=$(ls "$1"*".csv" )
echo $files
for filename in $files; do
  echo $i
  if [[ $i -eq 0 ]] ; then
    # copy csv headers from first file
    echo "first file"
    head -1 $filename > $output_file
  fi
  echo $i "common part"
  # copy csv without headers from other files
  tail -n +2 $filename >> $output_file
  i=$(( $i + 1 ))
done
