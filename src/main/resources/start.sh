#!/bin/bash
for i in {1..1000}
do
TIME=`date +"%H"`
PRIORITY=$(($RANDOM % 8))
case $PRIORITY in
0)
  STATUS="panic"
  ;;
1)
  STATUS="alert"
  ;;
2)
  STATUS="crit"
  ;;
3)
  STATUS="error"
  ;;
4)
  STATUS="warning"
  ;;
5)
  STATUS="notice"
  ;;
6)
  STATUS="info"
  ;;
7)
  STATUS="debug"
esac
printf '%s\n' $TIME $PRIORITY $STATUS | paste -sd ','
done > /home/cloudera/workspace/spark/src/main/resources/data.csv

