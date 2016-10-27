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

rm -rf target/site/home
mkdir -p target/site/

hash rsync 2> /dev/null

if [ $? -eq 0 ]; then
  rsync -avq docs/site/home target/site --exclude template
else
  cp -R docs/site/home target/site
  rm -rf target/site/home/template
fi

pushd docs/site/

for filename in home/*.html; do
  sed -e "/!!!!!BODY!!!!!/ r $filename" home/template/header-footer.html -e /!!!!!BODY!!!!!/d > "../../target/site/${filename}"
done

popd

echo "Home page site generated to $(cd target/site/home ; pwd)"
