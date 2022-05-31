#!/bin/bash
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

set -e

function usage {
  echo -e "\nUsage: $(basename "$0") <host> <port> <max retry> [OPTIONS]" \
          "\n\nCurls a Gremlin Server HTTP endpoint until success to signal that the server is ready, or until max number of retries reached to signal failure." \
          "\n\nOptions:" \
          "\n\t<host> \t\t Gremlin server host." \
          "\n\t<host> \t\t Gremlin server port." \
          "\n\t<max retry> \t Number of times to retry connection, entering 0 or less means no retry limit." \
          "\n\t-h, --help \t Show this message." \
          "\n"
}

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    usage
    exit 0
fi

host="$1"; shift
port="$1"; shift
max_retries="$1"; shift

# Setting max retry to 0 or less means no retry limit.
if [ "$max_retries" -le 0 ] ; then
  max_retries=-1
fi

counter=0
until curl "$host:$port?gremlin=100-1"; do
  if [ "$counter" -eq "$max_retries" ] ; then
    echo "Gremlin server is unavailable - exiting: max retries $max_retries reached, please check server setup or increase max retry value"
    exit 1
  fi
  >&2 echo "Gremlin server is unavailable - sleeping"
  sleep 1
  (( counter++ )) || true
done

>&2 echo "Gremlin Server is up - executing command"
exec "$@"
