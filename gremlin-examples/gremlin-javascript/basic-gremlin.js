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

const serverUrl = 'ws://localhost:8182/gremlin';
const vertexLabel = 'person';

async function main() {
    const dc = new DriverRemoteConnection(serverUrl);
    const g = traversal().withRemote(dc);

    // Basic Gremlin: adding and retrieving data
    const v1 = await g.addV(vertexLabel).property('name','marko').next();
    const v2 = await g.addV(vertexLabel).property('name','stephen').next();
    const v3 = await g.addV(vertexLabel).property('name','vadas').next();

    // Be sure to use a terminating step like next() or iterate() so that the traversal "executes"
    // Iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
    await g.V(v1.value).addE('knows').to(v2.value).property('weight',0.75).iterate();
    await g.V(v1.value).addE('knows').to(v3.value).property('weight',0.75).iterate();

    // Retrieve the data from the "marko" vertex
    const marko = await g.V().has(vertexLabel,'name','marko').values('name').toList();
    console.log("name: " + marko[0]);

    // Find the "marko" vertex and then traverse to the people he "knows" and return their data
    const peopleMarkoKnows = await g.V().has(vertexLabel,'name','marko').out('knows').values('name').toList();
    peopleMarkoKnows.forEach((person) => {
        console.log("marko knows " + person);
    });

    await dc.close();
}

main();