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
if [[ -d ../target/site/home ]]; then
  rm -r ../target/site/home
fi
mkdir -p ../target/site/
cp -R home ../target/site

for filename in home/*.html; do
    sed -e "/!!!!!BODY!!!!!/ r $filename" home/template/header-footer.html -e /!!!!!BODY!!!!!/d > "../target/site/$filename"
done

echo "Home page site generated to target/site/home"