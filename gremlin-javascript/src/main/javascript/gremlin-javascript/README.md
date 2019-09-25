<!--

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

-->

# JavaScript Gremlin Language Variant

[Apache TinkerPop™][tk] is a graph computing framework for both graph databases (OLTP) and graph analytic systems
(OLAP). [Gremlin][gremlin] is the graph traversal language of TinkerPop. It can be described as a functional,
data-flow language that enables users to succinctly express complex traversals on (or queries of) their application's
property graph.

Gremlin-Javascript implements Gremlin within the JavaScript language and can be used on Node.js.

```bash
npm install gremlin
```

Gremlin-Javascript is designed to connect to a "server" that is hosting a TinkerPop-enabled graph system. That "server" 
could be [Gremlin Server][gs] or a [remote Gremlin provider][rgp] that exposes protocols by which Gremlin-Javascript 
can connect.

A typical connection to a server running on "localhost" that supports the Gremlin Server protocol using websockets 
looks like this:

```javascript
const gremlin = require('gremlin');
const traversal = gremlin.process.AnonymousTraversalSource.traversal;
const DriverRemoteConnection = gremlin.driver.DriverRemoteConnection;

const g = traversal().withRemote(new DriverRemoteConnection('ws://localhost:8182/gremlin'));
```

Once "g" has been created using a connection, it is then possible to start writing Gremlin traversals to query the 
remote graph:

```javascript
g.V().hasLabel('person').values('name').toList()
  .then(names => console.log(names));

const names = await g.V().hasLabel('person').values('name').toList();
console.log(names);
```

Please see the [reference documentation][docs] at Apache TinkerPop for more information.

NOTE that versions suffixed with "-rc" are considered release candidates (i.e. pre-alpha, alpha, beta, etc.) and 
thus for early testing purposes only.

[tk]: http://tinkerpop.apache.org
[gremlin]: http://tinkerpop.apache.org/gremlin.html
[docs]: http://tinkerpop.apache.org/docs/current/reference/#gremlin-javascript
[gs]: http://tinkerpop.apache.org/docs/current/reference/#gremlin-server
[rgp]: http://tinkerpop.apache.org/docs/current/reference/#connecting-rgp
