#!/bin/bash
#
#
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
#

USERNAME=$1

if [ "${USERNAME}" == "" ]; then
  echo "Please provide a SVN username."
  echo -e "\nUsage:\n\t$0 <username>\n"
  exit 1
fi

read -s -p "Password for SVN user ${USERNAME}: " PASSWORD
echo

SVN_CMD="svn --no-auth-cache --username=${USERNAME} --password=${PASSWORD}"

rm -rf target/svn
bin/generate-home.sh

mkdir -p target/svn
${SVN_CMD} co --depth immediates https://svn.apache.org/repos/asf/tinkerpop/site target/svn