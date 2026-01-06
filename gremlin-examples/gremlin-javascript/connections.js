/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

const gremlin = require('gremlin');
const traversal = gremlin.process.AnonymousTraversalSource.traversal;
const DriverRemoteConnection = gremlin.driver.DriverRemoteConnection;
const serializer = gremlin.structure.io.graphserializer;

const serverUrl = 'ws://localhost:8182/gremlin';
const vertexLabel = 'connection';

async function main() {
    await withRemote();
    await withConfigs();
}

async function withRemote() {
    // Connecting to the server
    const dc = new DriverRemoteConnection(serverUrl);
    const g = traversal().withRemote(dc);

    // Simple query to verify connection
    const v = await g.addV(vertexLabel).iterate();
    const count = await g.V().hasLabel(vertexLabel).count().next();
    console.log("Vertex count: " + count.value);

    // Cleanup
    await dc.close();
}

async function withConfigs() {
    // Connecting and customizing configurations
    const dc = new DriverRemoteConnection(serverUrl, {
        mimeType: 'application/vnd.gremlin-v3.0+json',
        reader: serializer,
        writer: serializer,
        rejectUnauthorized: false,
        traversalSource: 'g',
    });
    const g = traversal().withRemote(dc);

    const v = await g.addV(vertexLabel).iterate();
    const count = await g.V().hasLabel(vertexLabel).count().next();
    console.log("Vertex count: " + count.value);

    await dc.close();
}

main();