#!/bin/bash

#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing,
#software distributed under the License is distributed on an
#"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#KIND, either express or implied.  See the License for the
#specific language governing permissions and limitations
#under the License.

function cleanup {
  if [ -n "$(docker ps -q -f name=glv-examples)" ]
  then
    echo -n "Shutting down container: "
    docker stop glv-examples
  fi
  if [ -n "$(docker ps -q -f name=glv-examples-modern)" ]
  then
    echo -n "Shutting down container: "
    docker stop glv-examples-modern
  fi
}
trap cleanup EXIT

open -a Docker
docker pull tinkerpop/gremlin-server
docker run -d --rm -p 8182:8182 --name glv-examples --health-cmd="curl -f http://localhost:8182/ || exit 1" --health-interval=5s --health-timeout=3s tinkerpop/gremlin-server

echo
echo "Not having Docker open initially or occupying port 8182 may cause an error loop. If so, simply restart this script."
echo -n "Starting Gremlin server on port 8182..."
until docker inspect --format '{{.State.Health.Status}}' glv-examples | grep -q "healthy"; do
     echo -n "."
     sleep 1
done
echo

cd gremlin-driver/src/main/java/examples || exit
mvn clean install

echo
echo "Running Java examples"
java -cp target/run-examples-shaded.jar examples.Connections
java -cp target/run-examples-shaded.jar examples.BasicGremlin
cd ../../../../.. || exit

echo
echo "Running python examples:"
python3 gremlin-python/src/main/python/examples/connections.py
python3 gremlin-python/src/main/python/examples/basic_gremlin.py

echo
echo "Running JavaScript examples:"
npm install --prefix gremlin-javascript/examples
node gremlin-javascript/examples/connections.js
node gremlin-javascript/examples/basic-gremlin.js

echo
echo "Running .NET examples:"
dotnet run --project gremlin-dotnet/Examples/Connections
dotnet run --project gremlin-dotnet/Examples/BasicGremlin

echo
echo "Running Go examples:"
cd gremlin-go/examples || exit
go run connections.go
go run basic_gremlin.go
cd ../.. || exit

echo
echo -n "Shutting down container: "
docker stop glv-examples

echo
docker run -d --rm -p 8182:8182 --name glv-examples-modern --health-cmd="curl -f http://localhost:8182/ || exit 1" --health-interval=5s --health-timeout=3s tinkerpop/gremlin-server conf/gremlin-server-modern.yaml
echo -n "Starting Modern server on port 8182..."
until docker inspect --format '{{.State.Health.Status}}' glv-examples-modern | grep -q "healthy"; do
     echo -n "."
     sleep 1
done
echo

echo
echo "Running Java traversals"
cd gremlin-driver/src/main/java/examples || exit
java -cp target/run-examples-shaded.jar examples.ModernTraversals
cd ../../../../.. || exit

echo
echo "Running python traversals:"
python3 gremlin-python/src/main/python/examples/modern_traversals.py

echo
echo "Running JavaScript traversals:"
npm install --prefix gremlin-javascript/examples
node gremlin-javascript/examples/modern-traversals.js

echo
echo "Running .NET traversals:"
dotnet run --project gremlin-dotnet/Examples/ModernTraversals

echo
echo "Running Go traversals:"
cd gremlin-go/examples || exit
go run modern_traversals.go
cd ../.. || exit

echo
echo -n "Shutting down container: "
docker stop glv-examples-modern
