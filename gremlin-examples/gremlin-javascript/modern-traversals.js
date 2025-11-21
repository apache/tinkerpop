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
const __ = gremlin.process.statics;
const DriverRemoteConnection = gremlin.driver.DriverRemoteConnection;
const p = gremlin.process.P;
const t = gremlin.process.t;

const serverUrl = 'ws://localhost:8182/gremlin';

async function main() {
    /*
    This example requires the Modern toy graph to be preloaded upon launching the Gremlin server.
    For details, see https://tinkerpop.apache.org/docs/current/reference/#gremlin-server-docker-image and use
    conf/gremlin-server-modern.yaml.
    */
    const dc = new DriverRemoteConnection(serverUrl);
    const g = traversal().withRemote(dc);

    const e1 = await g.V(1).bothE().toList(); // (1)
    const e2 = await g.V(1).bothE().where(__.otherV().hasId(2)).toList(); // (2)
    const v1 = await g.V(1).next();
    const v2 = await g.V(2).next();
    const e3 = await g.V(v1.value).bothE().where(__.otherV().is(v2.value)).toList(); // (3)
    const e4 = await g.V(v1.value).outE().where(__.inV().is(v2.value)).toList(); // (4)
    const e5 = await g.V(1).outE().where(__.inV().has(t.id, p.within(2,3))).toList(); // (5)
    const e6 = await g.V(1).out().where(__.in_().hasId(6)).toList(); // (6)

    console.log("1: " + e1.toString());
    console.log("2: " + e2.toString());
    console.log("3: " + e3.toString());
    console.log("4: " + e4.toString());
    console.log("5: " + e5.toString());
    console.log("6: " + e6.toString());
    
    /*
    1. There are three edges from the vertex with the identifier of "1".
    2. Filter those three edges using the where()-step using the identifier of the vertex returned by otherV() to
       ensure it matches on the vertex of concern, which is the one with an identifier of "2".
    3. Note that the same traversal will work if there are actual Vertex instances rather than just vertex
       identifiers.
    4. The vertex with identifier "1" has all outgoing edges, so it would also be acceptable to use the directional
       steps of outE() and inV() since the schema allows it.
    5. There is also no problem with filtering the terminating side of the traversal on multiple vertices, in this
       case, vertices with identifiers "2" and "3".
    6. There’s no reason why the same pattern of exclusion used for edges with where() can’t work for a vertex
       between two vertices.
    */

   await dc.close();
}

main();