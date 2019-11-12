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

FROM ubuntu:xenial

LABEL maintainer="Daniel Kuppitz <me@gremlin.guru>"

RUN apt-get update
RUN apt-get -y install software-properties-common python-software-properties apt-transport-https curl dpkg
RUN add-apt-repository ppa:openjdk-r/ppa
RUN sh -c 'curl -s https://packages.microsoft.com/config/ubuntu/16.04/packages-microsoft-prod.deb -o packages-microsoft-prod.deb'
RUN sh -c 'dpkg -i packages-microsoft-prod.deb'
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF
RUN apt-get install apt-transport-https gnupg ca-certificates
RUN sh -c 'echo "deb https://download.mono-project.com/repo/ubuntu stable-xenial main" | tee /etc/apt/sources.list.d/mono-official-stable.list'
RUN apt-get update

# include both java 8/11 so that we can use the same docker image for future builds on that version of the jdk as we do
# for the older release branches. the java version to use is just controlled by JAVA_HOME hardcoded below
RUN apt-get install -y openjdk-8-jdk openjdk-11-jdk gawk git maven openssh-server subversion zip
RUN apt-get install -y --force-yes dotnet-sdk-2.2 mono-devel

# python3 on xenial install 3.5.2 which is insufficient for newer versions of python/typing#259 so
# custom build and install 3.5.3 and upgrade pip along the way. this could be resolved by using bionic
# but trying to keep all of our release branches on the same docker image and the older versions sorta
# suit 3.3.x and 3.4.x
RUN apt-get install -y --force-yes python python-dev python-pip build-essential checkinstall zlib1g-dev libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev
RUN cd /opt && wget https://www.python.org/ftp/python/3.5.3/Python-3.5.3.tgz && tar -xvf Python-3.5.3.tgz
RUN cd /opt/Python-3.5.3 && ./configure && make && make install
RUN ln -sf /usr/bin/python3.5.3 /usr/bin/python3
RUN pip install --upgrade pip
RUN pip install virtualenv virtualenvwrapper

RUN rm -rf /var/lib/apt/lists/* /var/cache/openjdk-8-jdk
RUN rm -rf /var/lib/apt/lists/* /var/cache/openjdk-11-jdk

RUN sed -i 's@PermitRootLogin without-password@PermitRootLogin yes@' /etc/ssh/sshd_config
RUN sed -i 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuuid.so@g' /etc/pam.d/sshd
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -N '' \
    && cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys

RUN sed -i 's/.*"$PS1".*/# \0/' ~/.bashrc

RUN echo "Host *" >> ~/.ssh/config
RUN echo "  UserKnownHostsFile /dev/null" >> ~/.ssh/config
RUN echo "  StrictHostKeyChecking no" >> ~/.ssh/config
