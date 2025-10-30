////
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
////
### CMDB Graph API (Java, Micronaut 4, Gremlin Server)

This is a language/framework example project implementing the DevOps CMDB + Incident Knowledge Graph API described in `../spec/spec.md`. It connects remotely to a Gremlin Server and does not use any embedded graph.

Key facts:
- Java 17
- Micronaut 4
- Apache TinkerPop 3.7.4 (gremlin-driver)
- Deterministic dataset: `../spec/cmdb-data.gremlin`

Quick start (Gremlin Server via Docker):
- From this directory (`docs/upgrade/java`):
  - Make helper executable (first time only): `chmod +x ./bin/gremlin-dev.sh`
  - Start server: `./bin/gremlin-dev.sh up`
  - Wait until ready: `./bin/gremlin-dev.sh wait`
  - Load dataset: `./bin/gremlin-dev.sh seed`
  - Or all-in-one reset: `./bin/gremlin-dev.sh reset`
  - Stop server: `./bin/gremlin-dev.sh down`

TEMPORARY QUICK START STEPS ON STARTUP:
```
# connect to the running container after: ./bin/gremlin-dev.sh up`
docker exec -it cmdb-gremlin /bin/bash

# run the following in the container  
sed -i "s#org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer#org.apache.tinkerpop.gremlin.server.channel.WsAndHttpChannelizer#" "conf/gremlin-server.yaml"
exit

# restart the container
docker container restart cmdb-gremlin
```

The helper uses the Docker image `tinkerpop/gremlin-server:3.7.4` and loads data by posting each line of `../spec/cmdb-data.gremlin` to the Gremlin Server HTTP endpoint. No Gremlin Console is required.

Manual alternative (without the helper script):
- Start server: `docker run -d --name cmdb-gremlin -p 8182:8182 tinkerpop/gremlin-server:3.7.4`
- Wait for readiness (simple probe): `curl -sS -H 'Content-Type: application/json' -X POST http://localhost:8182/gremlin --data '{"gremlin":"g.V().limit(1)"}'`
- Load dataset lines over HTTP (bash example):
  ```bash
  while IFS= read -r line; do \
    [[ -z "$line" || "$line" =~ ^// || "$line" =~ ^# ]] && continue; \
    json=$(printf '%s' "$line" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'); \
    curl -sS -H 'Content-Type: application/json' -X POST http://localhost:8182/gremlin --data "{\"gremlin\":\"$json\"}" >/dev/null; \
  done < ../spec/cmdb-data.gremlin
  ```

Build & Run API:
- Build: `mvn -q -f ./pom.xml clean package`
- Run: `mvn -q -f ./pom.xml mn:run`
- Run Integration Tests: `mvn test -Dtest=ServiceIT`

Endpoints (examples):
- `GET http://localhost:8080/health/ready`
- `GET http://localhost:8080/incidents/{incidentId}/blastRadius?depth=2`
- `GET http://localhost:8080/services/{serviceId}/dependencies?depth=2`
- `GET http://localhost:8080/services/{serviceId}/owners`
- `GET http://localhost:8080/deployments/{deployId}/timeline`
- `POST http://localhost:8080/alerts` with JSON `{ "alertId": "a1", "source": "prom", "metric": "cpu", "firedAt": 1700000000000 }`

Configuration:
- See `src/main/resources/application.yml` for Gremlin connection settings.

License:
- All files in this example are licensed under the Apache License, Version 2.0.
