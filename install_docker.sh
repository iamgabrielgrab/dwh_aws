echo "install docker"
sudo apt update
sudo apt install apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
apt-cache policy docker-ce
echo "*** done with: apt-cache policy docker-ce *****************************"
sudo apt install docker-ce
echo "*** done with: sudo apt install docker-ce *****************************"
# sudo systemctl status docker
echo "*** install docker-compose *****************"
sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
# echo "docker-compose version:"
# docker-compose --version
echo "create network **"
sudo docker network create --subnet=172.123.0.0/16 dwh-network