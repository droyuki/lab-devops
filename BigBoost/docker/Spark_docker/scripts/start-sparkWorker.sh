#!/bin/sh -x
(
# start ssh
service sshd start
service ssh start

# start spark
/spark-1.5.0-bin-hadoop2.6/sbin/start-slave.sh spark://$(getent hosts bigboost-spark | awk '{ print $1 }'):7077

# execute a foreground program
while true; do sleep 1000; done
)>/opt/start_spark.log