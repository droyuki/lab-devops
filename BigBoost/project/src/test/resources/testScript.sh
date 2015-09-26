#!/bin/sh
echo "Start receiving RDD..."
while read LINE; do
    echo ${LINE} >> rddLog.txt
done