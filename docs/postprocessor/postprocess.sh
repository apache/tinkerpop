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

pushd "$(dirname $0)/../.." > /dev/null

TP_VERSION=$(cat pom.xml | grep -A1 '<artifactId>tinkerpop</artifactId>' | grep -o 'version>[^<]*' | grep -o '>.*' | cut -d '>' -f2 | head -n1)

if [ -d "target/docs" ]; then
  find target/docs -name index.html | while read file ; do
    awk -f "docs/postprocessor/processor.awk" "${file}"                   \
      | perl -0777 -pe 's/<span class="comment">\/\*\n \*\/<\/span>//igs' \
      | sed "s/x\.y\.z/${TP_VERSION}/g"                                   \
      > "${file}.tmp" && mv "${file}.tmp" "${file}"
  done
fi

popd > /dev/null
