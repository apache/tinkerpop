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

FROM --platform=linux/amd64 ubuntu:focal

LABEL maintainer="Daniel Kuppitz <me@gremlin.guru>"

RUN apt-get update
RUN apt-get -y install software-properties-common python3-software-properties apt-transport-https curl dpkg netcat net-tools iproute2
RUN add-apt-repository ppa:openjdk-r/ppa
RUN sh -c 'curl -s https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -o packages-microsoft-prod.deb'
RUN sh -c 'dpkg -i packages-microsoft-prod.deb'
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
RUN apt-get install apt-transport-https gnupg ca-certificates
RUN sh -c 'echo "deb https://download.mono-project.com/repo/ubuntu stable-focal main" | tee /etc/apt/sources.list.d/mono-official-stable.list'
RUN apt-get update

RUN apt-get install -y openjdk-11-jdk gawk git maven openssh-server subversion zip
RUN apt-get install -y --force-yes dotnet-sdk-8.0 mono-devel

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get install -y python3 python3.8 python3-dev python3.8-dev python3-pip build-essential checkinstall zlib1g-dev libreadline-gplv2-dev \
    libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libkrb5-dev krb5-user

# make python3 be python 3.8
RUN rm /usr/bin/python3
RUN ln -s python3.8 /usr/bin/python3
RUN python3 -m pip install --upgrade pip
RUN pip install virtualenv
RUN pip install virtualenvwrapper --no-deps

RUN rm -rf /var/lib/apt/lists/* /var/cache/openjdk-11-jdk

RUN sed -i 's@PermitRootLogin without-password@PermitRootLogin yes@' /etc/ssh/sshd_config
RUN sed -i 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuuid.so@g' /etc/pam.d/sshd
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' \
    && cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys

RUN sed -i 's/.*"$PS1".*/# \0/' ~/.bashrc

RUN echo "Host *" >> ~/.ssh/config
RUN echo "  UserKnownHostsFile /dev/null" >> ~/.ssh/config
RUN echo "  StrictHostKeyChecking no" >> ~/.ssh/config
