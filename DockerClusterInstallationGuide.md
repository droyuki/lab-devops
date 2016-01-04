# Docker Cluster Installation Guide
---
Java
###### 前置作業
#### 1. Install Docker. (Follow official website.)
```sh
#set root password
$ sudo passwd root
```
Create the docker group and add your user.
```sh
$ sudo usermod -aG docker username
#log out and log back in.
#Verify your work by running docker without sudo
```

#### 2. Install Docker machine. (https://docs.docker.com/machine/install-machine/)
```sh
$ curl -L https://github.com/docker/machine/releases/download/v0.5.3/docker-machine_linux-amd64 >/usr/local/bin/docker-machine
$ chmod +x /usr/local/bin/docker-machine
```

#### 3. pull swarm image
```sh
$ sudo docker pull swarm
```

----
#### 來吧
# 使用 Shipyard

1. 在Master上
```sh
$ curl -sSL https://shipyard-project.com/deploy | bash -s
```
2. 各節點上
```sh
curl -sSL https://shipyard-project.com/deploy | ACTION=node DISCOVERY=etcd://140.119.19.231:4001 bash -s

```
---

# 不使用Shipyard

#### Swarm 管理cluster前，所有節點上 docker daemon 的port都要改成 0.0.0.0:2375
```sh
# 修改/etc/default/docker 
DOCKER_OPTS="-H 0.0.0.0:2375 -H unix:///var/run/docker.sock"
#存檔, 重開Docker
$ sudo service docker restart
```

#### 方法1. 使用 Discovery service backend
其中一個node可同時當swarm manager

 1. 在任何一個節點執行swarm create, 得到token
```sh
$ sudo docker run --rm swarm create
```
2. 啟動swarm manager
```sh
#需2375以外的port
$ sudo docker run -d -p 2376:2375 swarm manage token://token_id
$ docker ps #check
```

3. 在所有要加入叢集的node上面执行 swarm join 
```sh
#請替換ip_address與token_id
$ sudo docker run --rm swarm join --addr=ip_address:2375 token://token_id
```

----
### 方法2. 建立cluster list
```sh
echo ip:2375 >> cluster
echo ip2:2375 >> cluster
```
 在Manager上執行
```sh
sudo docker run -d -p 2376:2375 -v $(pwd)/cluster:/tmp/cluster swarm manage file:///tmp/cluster
```
----
#### 安裝完成
```sh
$ docker -H 140.119.19.231:2376 run -d -p 50070:50070 --name=bigboost-sparkWorker3 --hostname=bigboost-spark --link=stormtimeseries/bigboost-spark:stormtimeseries/bigboost-spark droyuki/bigboost-spark:lab-v1 start-sparkWorker.sh
```

----
#### Useful scripts
```sh
#Get IP from HOSTNAME
$ getent hosts unix.stackexchange.com | awk '{ print $1 }'

#Get internal IP (rancher)
$ ip addr | grep inet | grep 10.42 | tail -1 | awk '{print $2}' | awk -F\/ '{print $1}'
```