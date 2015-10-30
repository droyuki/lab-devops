#!/bin/bash

action=$1
shift
args=""
while [ -n "$*" ] ; do
    arg=$1
    shift
    case "$arg" in
        --algo|-a)
            if [ -z "$1" ]; then
                echo "Option --algo requires 1 argument"
                exit 1
            fi  
            opt_conf=$1
            shift
            ;;  
        *)  
            args="$args $arg"
            ;;  
    esac
done

#if [ -n "$1" ]; then
#   echo "custom repo path to \"$1\""
#   repo_path="$1"
#   sed -r -i "s/(repo_path): (.*)/\1: $repo_path/g" config/SHE.yml
#fi
#host_docker0_ip=`ip addr show dev docker0 | awk -F'[ /]*' '/inet /{print $3}'`
#sed -r -i "s/(host_docker0_ip): (.*)/\1: $host_docker0_ip/g" config/SHE.yml
#echo $host_docker0_ip

kill_docker(){
docker rm $(docker ps -a -q)
}

check_config() {
    if [ ! -f "config/config.yml" ]; then
        echo "config.yml does not exist!!!"
        echo "Please copy config/config.yml.template to config.yml and customize your config."
        exit 1
    fi  
}
wait_dockerUp() {
    retryTime=10
    count=0
    
    #check spark
    until [ $count -ge $retryTime ]
    do
        response=$(curl --write-out %{http_code} --silent --output /dev/null http://$server:5555)
        if [ "$response" == "200" ]; then echo "Spark is up!"; break; fi
        count=$[$count+1]
        sleep 1
    done

    count=0
    #check hadoop
    until [ $count -ge $retryTime ]
    do
        response=$(curl --write-out %{http_code} --silent --output /dev/null http://$server:50070)
        if [ "$response" == "200" ]; then echo "Hadoop is up!"; break; fi
        count=$[$count+1]
        sleep 1
    done
}
start_boo() {
    start=`date +%s`
    check_config
    vagrant up --no-parallel
    wait_dockerUp
    end=`date +%s`
    runtime=$((end-start))
    echo "You spent $runtime sec to got a big boost !"
}

stop_boo() {
    vagrant halt
}

destroy_boo() {
    vagrant destroy -f
    rm -rf .vagrant
}

display_help() {
    cat <<EOF
Usage: $0 <command>
commands:
  help                          display this help text
  start                         start containers 
  stop                          stop containers
  destroy                       destroy all containers
  kill				rm all stopped container

EOF
}
case "$action" in
    help)
        display_help
        exit 0
        ;;
    start)
        start_boo
        ;;
    stop)
        stop_boo
        ;;
    destroy)
        destroy_boo
        ;;
    checkContainer)
        wait_dockerUp
        ;;
    kill)
        kill_docker
        ;;
    *)
        echo "Unknown command @@"
        echo
        display_help
        exit 1
        ;;
esac

