#!/bin/bash

head -1 ./201501-citibike-tripdata.csv > all_combined.csv
tail -n +2 -q ./*.csv >> all_combined.csv