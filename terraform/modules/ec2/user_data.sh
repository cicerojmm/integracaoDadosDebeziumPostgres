#!/bin/bash

DOCKER_COMPOSE_LOCAL_FILES=/docker-stack-debezium

# Update Ubuntu
apt-get update -y

# Install Docker
apt install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"

apt update -y
apt install -y docker-ce docker-ce-cli containerd.io
usermod -aG docker ubuntu

# Install Docker-Compose
sudo curl -L "https://github.com/docker/compose/releases/download/1.27.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose
ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# copy files s3 to local
sudo apt install awscli -y

mkdir $DOCKER_COMPOSE_LOCAL_FILES

values="${BUCKET_DOCKER_FILES}"
array=(`echo $values | sed 's/,/\n/g'`)

for index in $${!array[@]}
do
    sudo aws s3 cp $${array[index]} $DOCKER_COMPOSE_LOCAL_FILES
done

# start all docker
for dockerfile in $DOCKER_COMPOSE_LOCAL_FILES/*
do
  if [[ -f $dockerfile ]]; then
    docker-compose -f $dockerfile up -d
  fi;
done

echo "Done!!!"
