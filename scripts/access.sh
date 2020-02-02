#!/bin/bash

cmd='curl -s  http://10.0.0.238/getting-started.html'
irregular_cmd='curl -s http://10.0.0.238/irregular_access.html'
count=0
while :
do
  response=$(eval $cmd)
  echo ${response:0:100}
  num=`expr $RANDOM`

  if [ $count -eq 0 ] || [ $count -eq 1 ] || [ $count -eq 2 ] || [ $count -eq 3 ]; then
    if [ $(($num % 2)) -eq 0 ]; then
       response=$(eval $irregular_cmd)
       echo ${response:0:100}
    fi
  fi
  sleep 10
  echo $count
  count=`expr $count + 1`
  if [ $count -eq 5 ]; then
    count=0
  fi
done
