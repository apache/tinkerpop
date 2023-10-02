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

// Common imports as listed at reference/#gremlin-javascript-imports
const gremlin = require('gremlin');
const traversal = gremlin.process.AnonymousTraversalSource.traversal;
const __ = gremlin.process.statics;
const DriverRemoteConnection = gremlin.driver.DriverRemoteConnection;
const column = gremlin.process.column;
const direction = gremlin.process.direction;
const Direction = {
  BOTH: direction.both,
  IN: direction.in,
  OUT: direction.out,
  from_: direction.out,
  to: direction.in,
}
const p = gremlin.process.P;
const textp = gremlin.process.TextP;
const pick = gremlin.process.pick;
const pop = gremlin.process.pop;
const order = gremlin.process.order;
const scope = gremlin.process.scope;
const t = gremlin.process.t;
const cardinality = gremlin.process.cardinality;
const CardinalityValue = gremlin.process.CardinalityValue;
const serializer = gremlin.structure.io.graphserializer;
const testing = Direction.to;

async function main() {
    await connectionExample();
    await basicGremlinExample();
    await modernTraversalExample();
}



async function connectionExample() {
    // Connecting to the server
    let dc = new DriverRemoteConnection('ws://localhost:8182/gremlin');
    let g = traversal().withRemote(dc);

    // Connecting and customizing configurations
    g = traversal().withRemote(new DriverRemoteConnection('ws://localhost:8182/gremlin', {
        mimeType: 'application/vnd.gremlin-v3.0+json',
        reader: serializer,
        writer: serializer,
        rejectUnauthorized: false,
        traversalSource: 'g',
    }));
    
    // Cleanup
    await dc.close();
}

async function basicGremlinExample() {
    let dc = new DriverRemoteConnection('ws://localhost:8182/gremlin');
    let g = traversal().withRemote(dc);

    // Basic Gremlin: adding and retrieving data
    const v1 = await g.addV('person').property('name','marko').next();
    const v2 = await g.addV('person').property('name','stephen').next();
    const v3 = await g.addV('person').property('name','vadas').next();


    // Be sure to use a terminating step like next() or iterate() so that the traversal "executes"
    // Iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
    await g.V(v1.value).addE('knows').to(v2.value).property('weight',0.75).iterate();
    await g.V(v1.value).addE('knows').to(v3.value).property('weight',0.75).iterate();

    // Retrieve the data from the "marko" vertex
    const marko = await g.V().has('person','name','marko').values('name').toList();
    console.log("name: " + marko[0]);

    // Find the "marko" vertex and then traverse to the people he "knows" and return their data
    const peopleMarkoKnows = await g.V().has('person','name','marko').out('knows').values('name').toList();
    peopleMarkoKnows.forEach((person) => {
        console.log("marko knows " + person);
    });

    await dc.close();
}

async function modernTraversalExample() {
    /*
    This example requires the Modern toy graph to be preloaded upon launching the Gremlin server.
    For details, see https://tinkerpop.apache.org/docs/current/reference/#gremlin-server-docker-image and use
    conf/gremlin-server-modern.yaml.
    */

    let dc = new DriverRemoteConnection('ws://localhost:8182/gremlin');
    let g = traversal().withRemote(dc);

    e1 = await g.V(1).bothE().toList(); // (1)
    e2 = await g.V(1).bothE().where(__.otherV().hasId(2)).toList(); // (2)
    const v1 = await g.V(1).next();
    const v2 = await g.V(2).next();
    e3 = await g.V(v1.value).bothE().where(__.otherV().is(v2.value)).toList(); // (3)
    e4 = await g.V(v1.value).outE().where(__.inV().is(v2.value)).toList(); // (4)
    e5 = await g.V(1).outE().where(__.inV().has(t.id, p.within(2,3))).toList(); // (5)
    e6 = await g.V(1).out().where(__.in_().hasId(6)).toList(); // (6)

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