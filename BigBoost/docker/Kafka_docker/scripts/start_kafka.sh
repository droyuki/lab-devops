#!/bin/sh -x
(
# read the parameters
while [[ $# > 1 ]]
do
    key="$1"
    case $key in
        -t|--topics)
            topics="$2"
            shift
            ;;  
        -hoip|--host_bridge_ip)
            host_bridge_ip="$2"
            shift
            ;;  
        -mnt|--docker_mnt_path)
            mnt_path="$2"
            shift
            ;;  
        *)  
            # unknown option
            ;;  
    esac
    shift
done


# change host.name to Kafka container domain
# avoid host VM can not consumer the Kafka container
#Kafka_IP=$(hostname -i)
Kafka_IP=$(hostname)
sed -r -i "s/#(host.name)=(.*)/\1=$Kafka_IP/g" $KAFKA_HOME/config/server.properties
sed -r -i "s/(zookeeper.connect)=(.*)/\1=$Kafka_IP:2181\/kafka/g" $KAFKA_HOME/config/server.properties
sed -r -i "s/#(advertised.host.name)=(.*)/\1=$host_bridge_ip/g" $KAFKA_HOME/config/server.properties
sed -r -i "s/#(advertised.port)=(.*)/\1=9092/g" $KAFKA_HOME/config/server.properties

# execute Zookeeper & Kafka server
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
# wait server is already
retryTime=10
count=0
until [ $count -ge $retryTime ]
do
    $KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper bigboost-kafka:2181
    if [ $? -eq 0 ]; then break; fi
    count=$[$count+1]
    sleep 1
done

topics=${topics%\"}
topics=${topics#\"}
# create topic
if [ -n "$topics" ]; then
    for topic in $topics
    do  
        echo "=== create $topic topic ==="
        ext="${topic##*.}"
        if [ "$ext" == "proc" ]; then
            $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper bigboost-kafka:2181 --replication-factor 1 --partitions 3 --config retention.ms=8640000000 --config segment.ms=8640000000 --topic $topic
        elif [ "$ext" == "even" ] || [ "$ext" == "odd" ]; then
            $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper bigboost-kafka:2181 --replication-factor 1 --partitions 3 --config retention.ms=31536000000 --config segment.ms=31536000000 --topic $topic
        else
            $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper bigboost-kafka:2181 --replication-factor 1 --partitions 3 --topic $topic
        fi  
    done
else
    echo "=== create default topic \"test\" ==="
    $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper bigboost-kafka:2181 --replication-factor 1 --partitions 3 --topic test
fi

# execute ssh as a foreground program
/usr/sbin/sshd -D
) > start_kafka.log
