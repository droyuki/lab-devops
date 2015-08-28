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

start_she() {
    start=`date +%s`
    check_config
    vagrant up --no-parallel
    if [ -n "$opt_conf" ]; then
        wait_dockerUp
        scripts/sparkJobSubmit.sh $opt_conf
    fi
    end=`date +%s`
    runtime=$((end-start))
    echo "You spent $runtime sec to got a big boost !"
}

stop_she() {
    vagrant halt
}

destroy_she() {
    vagrant destroy -f
    rm -rf .vagrant
}

display_help() {
    cat <<EOF
Usage: $0 <command>
commands:
  help                          display this help text
  start                         start sherlock holmes engine 
  stop                          stop sherlock holmes engine
  destroy                       destroy sherlock holmes engine (all container)
  updateCheck                   check the docker images is up to date or not
  packer                        tar the SHE folder content to she.tar.gz
  kill				rm all stopped container

start options:
  --algo, -a <algorithm name>    start spark program after SHE start
                                ex: --algo DDI
                                ex: --algo help (display algorithm name)
EOF
}
case "$action" in
    help)
        display_help
        exit 0
        ;;
    start)
        start_she
        ;;
    stop)
        stop_she
        ;;
    destroy)
        destroy_she
        ;;
    updateCheck)
        update_check
        ;;
    checkContainer)
        wait_dockerUp
        ;;
    kill)
        kill_docker
        ;;
    packer)
        packer_she
        ;;
    *)
        echo "Unknown command"
        echo
        display_help
        exit 1
        ;;
esac

