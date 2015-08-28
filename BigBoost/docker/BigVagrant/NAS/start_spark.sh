#!/bin/sh -x
(
    
# start ssh
service sshd start

# start spark
/start-master.sh
/start-worker.sh

# execute a foreground program
while true; do sleep 1000; done
)>/opt/start_spark.log
