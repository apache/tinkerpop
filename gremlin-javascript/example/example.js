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
const column = gremlin.process.column
const direction = gremlin.process.direction
const Direction = {
  BOTH: direction.both,
  IN: direction.in,
  OUT: direction.out,
  from_: direction.in,
  to: direction.out,
}
const p = gremlin.process.P
const textp = gremlin.process.TextP
const pick = gremlin.process.pick
const pop = gremlin.process.pop
const order = gremlin.process.order
const scope = gremlin.process.scope
const t = gremlin.process.t
const cardinality = gremlin.process.cardinality
const CardinalityValue = gremlin.process.CardinalityValue
const serializer = gremlin.structure.io.graphserializer


let g = traversal().withRemote(new DriverRemoteConnection('ws://localhost:8182/gremlin'));
console.log(g);

// Connecting to the server with a configurations file
g = traversal().withRemote("gremlin-driver/src/main/java/example/conf/remote-graph.properties"); // make duplicate?
console.log(g);

// Connecting and customizing configurations
g = traversal().withRemote(new DriverRemoteConnection('ws://localhost:8182/gremlin', {
    mimeType: 'application/vnd.gremlin-v3.0+json',
    reader: serializer,
    writer: serializer,
    rejectUnauthorized: false,
    traversalSource: 'g',


}));

// Transaction example (error not supported?)
g = traversal().withRemote(new DriverRemoteConnection('ws://localhost:8182/gremlin'));
const tx = g.tx(); // create a Transaction

// spawn a new GraphTraversalSource binding all traversals established from it to tx
const gtx = tx.begin();

// execute traversals using gtx occur within the scope of the transaction held by tx. the
// tx is closed after calls to commit or rollback and cannot be re-used. simply spawn a
// new Transaction from g.tx() to create a new one as needed. the g context remains
// accessible through all this as a sessionless connection.
Promise.all([
  gtx.addV("person").property("name", "jorge").iterate(),
  gtx.addV("person").property("name", "josh").iterate()
]).then(() => {
  return tx.commit();
}).catch(() => {
  return tx.rollback();
});

console.log(g.V());