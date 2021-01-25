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

for branch in $(git branch -r | grep -Po 'TINKERPOP[3]?-.*')
do
  jira=`sed 's/TINKERPOP3/TINKERPOP/' <<< $branch | grep -Po 'TINKERPOP-[0-9]*'`
  curl -s https://issues.apache.org/jira/browse/$jira > /tmp/$jira
  status=`cat /tmp/$jira | grep -A1 status-val | grep -Po '(?<=>)[^<]*(?=)' | head -n1`
  title=`cat /tmp/$jira | grep -Po '(?<=<title>).*(?=</title>)' | head -n1 | recode html..ascii | sed 's/ - ASF JIRA//'`
  if [ "$status" == "Closed" ]; then
    if [ "$1" == "--delete" ]; then
      git push origin --delete $branch
      git branch -D $branch
      git branch -r -D origin/$branch
    else
      echo "$branch -- $title"
    fi
  fi
  rm /tmp/$jira
done
