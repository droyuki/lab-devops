#!/bin/sh -x
(
# start ssh
service sshd start

# start spark
/spark-1.4.1-bin-hadoop2.6/sbin/start-master.sh
/spark-1.4.1-bin-hadoop2.6/sbin/start-slave.sh spark://bigboost-spark:7077

# execute a foreground program
while true; do sleep 1000; done
)>/opt/start_spark.log
