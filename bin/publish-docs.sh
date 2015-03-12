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

# TODO: i guess this script should take arguments for svn username/password.  svn commands should probably run with --non-interactive option
USERNAME=XXX
rm -rf target
mkdir -p target/svn
svn --no-auth-cache --username=$USERNAME co https://svn.apache.org/repos/asf/incubator/tinkerpop/site/ target/svn

# TODO: how do we get the version number from the pom into this guy?
# TODO: maybe this should be smart about checking for if directories exist before removing
cd target/svn
svn --no-auth-cache --username=$USERNAME rm site/docs/3.0.0-SNAPSHOT
svn --no-auth-cache --username=$USERNAME rm site/javadocs/3.0.0-SNAPSHOT
svn --no-auth-cache --username=$USERNAME commit . -m "Docs for TinkerPop 3.0.0-SNAPSHOT are being replaced."
cd ../..

docs/preprocessor/preprocess.sh && mvn process-resources -Dasciidoc
mvn process-resources -Djavadoc

mkdir -p target/svn/site/docs/3.0.0-SNAPSHOT
mkdir -p target/svn/site/javadocs/3.0.0-SNAPSHOT/core
mkdir -p target/svn/site/javadocs/3.0.0-SNAPSHOT/full

cp -R target/docs/htmlsingle/. target/svn/site/docs/3.0.0-SNAPSHOT
cp -R target/site/apidocs/core/. target/svn/site/javadocs/3.0.0-SNAPSHOT/core
cp -R target/site/apidocs/full/. target/svn/site/javadocs/3.0.0-SNAPSHOT/full

cd target/svn
svn --no-auth-cache --username=$USERNAME add * --force
svn --no-auth-cache --username=$USERNAME commit -m "Deploy docs for TinkerPop 3.0.0-SNAPSHOT"
cd ../..