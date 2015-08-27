#!/bin/bash
echo "Installing vagrant..."
sudo apt-get update
sudo apt-get -y install vagrant
echo "done!"

echo "Installing Docker..."
sudo apt-get -y install docker.io
sudo ln -sf /usr/bin/docker.io /usr/local/bin/docker
sudo sed -i '$acomplete -F _docker docker' /etc/bash_completion.d/docker.io
sudo service docker start
echo "done!"

echo "Grant docker access right to vagrant user"
sudo usermod -a -G docker vagrant

echo "Closing iptables..."
service iptables stop
chkconfig iptables off 
echo "OK!"
