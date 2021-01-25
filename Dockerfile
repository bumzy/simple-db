FROM ubuntu:14.04
MAINTAINER bumzycm "bumzycm@gmail.com"

RUN apt-get update
RUN apt-get install -y openjdk-6-jdk
RUN apt-get install -y ant