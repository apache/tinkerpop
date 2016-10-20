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

cd `dirname $0`/..

USERNAME=$1

if [ "${USERNAME}" == "" ]; then
  echo "Please provide a SVN username."
  echo -e "\nUsage:\n\t$0 <username>\n"
  exit 1
fi

if [ ! -d ../target/site/home ]; then
  bin/generate-home.sh || exit 1
  echo
fi

read -s -p "Password for SVN user ${USERNAME}: " PASSWORD
echo

SVN_CMD="svn --no-auth-cache --username=${USERNAME} --password=${PASSWORD}"

cd ..
rm -rf target/svn
mkdir -p target/svn

${SVN_CMD} co --depth files https://svn.apache.org/repos/asf/tinkerpop/site target/svn

cd target/svn

find ../site/home -mindepth 1 -maxdepth 1 -type d | xargs -n1 basename | xargs -r ${SVN_CMD} update

diff -rq ./ ../site/home/ | awk -f ../../bin/publish-docs.awk | sed 's/^\(.\) \//\1 /g' > ../publish-home.files

for file in $(cat ../publish-home.files | awk '/^[AU]/ {print $2}')
do
  if [ -d "../site/home/${file}" ]; then
    mkdir -p "${file}" && cp -r "../site/home/${file}"/* "$_"
  else
    mkdir -p "`dirname ${file}`" && cp "../site/home/${file}" "$_"
  fi
done

cat ../publish-home.files | awk '/^A/ {print $2}' | xargs -r svn add --parents
cat ../publish-home.files | awk '/^D/ {print $2}' | xargs -r svn delete

CHANGES=$(cat ../publish-home.files | wc -l)

if [ ${CHANGES} -gt 0 ]; then
  ${SVN_CMD} commit -m "Deploy TinkerPop homepage"
fi

