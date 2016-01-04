#!/bin/sh -x
(
# start ssh
service sshd start
service ssh start

#link
sed '1d' /etc/hosts
echo "$(ip addr | grep inet | grep 10.42 | tail -1 | awk '{print $2}' | awk -F\/ '{print $1}') bigboost-spark" >> /etc/hosts

# start spark
/spark-1.5.0-bin-hadoop2.6/sbin/start-slave.sh spark://bigboost-spark:7077

# execute a foreground program
while true; do sleep 1000; done
)>/opt/start_spark.log