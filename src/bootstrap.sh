#/bin/bash

sudo yum update -y
#sudo yum install -y python36u python36u-libs python36u-devel python36u-pip
sudo pip install --upgrade pip
sudo /usr/local/bin/pip install numpy pickle hyperopt
