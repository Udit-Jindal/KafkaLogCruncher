#!/bin/bash

for(( i=1; i <= $1; i++ ))
do
  name=$(hostname | cut -d'-' -f3,4,5)

  rm -f logs/producer-$i-$name.log

  screen -md -L -Logfile /adpushup/benchmark_scripts/logs/producer-$i-$name.log -S producer-$i /adpushup/benchmark_scripts/producer.sh producer-$i-$name
done