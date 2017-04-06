# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM ubuntu:trusty

MAINTAINER Daniel Kuppitz <me@gremlin.guru>

RUN apt-get update \
    && apt-get -y install software-properties-common python-software-properties apt-transport-https \
    && echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections \
    && add-apt-repository -y ppa:webupd8team/java \
    && sh -c 'echo "deb [arch=amd64] https://apt-mo.trafficmanager.net/repos/dotnet-release/ trusty main" > /etc/apt/sources.list.d/dotnetdev.list' \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 417A0893 \
    && apt-get update \
    && apt-get install -y oracle-java8-installer curl gawk git maven openssh-server dotnet-dev-1.0.1 subversion \
    && apt-get install -y python python-dev python-pip build-essential \
    && pip install virtualenv virtualenvwrapper \
    && pip install --upgrade pip \
    && rm -rf /var/lib/apt/lists/* /var/cache/oracle-jdk8-installer

RUN sed -i 's@PermitRootLogin without-password@PermitRootLogin yes@' /etc/ssh/sshd_config
RUN sed -i 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuuid.so@g' /etc/pam.d/sshd
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' \
    && cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

RUN sed -i 's/.*"$PS1".*/# \0/' ~/.bashrc
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ~/.bashrc

RUN echo "Host *" >> ~/.ssh/config
RUN echo "  UserKnownHostsFile /dev/null" >> ~/.ssh/config
RUN echo "  StrictHostKeyChecking no" >> ~/.ssh/config
