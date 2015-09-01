# -*- mode: ruby -*-
# vi: set ft=ruby :

$script = <<SCRIPT
echo "Installing vagrant..."
sudo wget "https://dl.bintray.com/mitchellh/vagrant/vagrant_1.7.4_x86_64.deb"
sudo dpkg -i /vagrant_1.7.4_x86_64.deb
echo "done!"
echo "Installing Docker..."
curl -sSL https://get.docker.com/ | sh
sudo usermod -aG docker vagrant
echo "Grant docker access right to vagrant user"
sudo usermod -aG docker vagrant
echo "Closing iptables..."
sudo ufw disable
echo "OK!"
SCRIPT

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.synced_folder './', '/lab-devops'
  config.vm.box_download_insecure = true
  config.vm.box = "ubuntu/trusty64"

  # If you're using laptop and WIFI network, mark out public_network since it can not obtain the DHCP IP.
  # The public_network works fine with wired network only in our environment.
  #config.vm.network "public_network"
  config.vm.network :"private_network", ip: "192.168.10.2"
  #you can add more cores or ram by modify  vb.customize["--memory", "4096", "--cpus", "2"] 
  config.vm.provision "shell", inline: $script
  config.vm.provider "virtualbox" do |vb|
    vb.customize ["modifyvm", :id, "--memory", "8192", "--cpus", "2"]
  end
end
