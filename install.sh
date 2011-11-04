#!/bin/sh

# make data directory (you can replace with symbolic link to elsewhere)
test -e data || mkdir data

# install matplotlib & numpy
sudo apt-get install python-matplotlib
sudo apt-get install python-numpy

# install install Python Twitter Tools
sudo easy_install twitter

