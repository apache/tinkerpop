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

using Gremlin;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Structure.IO.GraphSON;
using Gremlin.Net.Process.Traversal;

// Common imports
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using static Gremlin.Net.Process.Traversal.__;
using static Gremlin.Net.Process.Traversal.P;
using static Gremlin.Net.Process.Traversal.Order;
using static Gremlin.Net.Process.Traversal.Operator;
using static Gremlin.Net.Process.Traversal.Pop;
using static Gremlin.Net.Process.Traversal.Scope;
using static Gremlin.Net.Process.Traversal.TextP;
using static Gremlin.Net.Process.Traversal.Column;
using static Gremlin.Net.Process.Traversal.Direction;
using static Gremlin.Net.Process.Traversal.Cardinality;
using static Gremlin.Net.Process.Traversal.CardinalityValue;
using static Gremlin.Net.Process.Traversal.T;


ConnectionExample();
BasicGremlinExample();
ModernTraversalExample();

static void ConnectionExample()
{
    var server = new GremlinServer("localhost", 8182);

    // Connecting to the server
    using var remoteConnection = new DriverRemoteConnection(new GremlinClient(server), "g");

    // Connecting to the server with customized configurations
//    using var remoteConnection = new DriverRemoteConnection(new GremlinClient(
//        new GremlinServer(hostname:"localhost", port:8182, enableSsl:false, username:"", password:"")), "g");

    // Specifying a serializer
//    var client = new GremlinClient(server, new GraphSON3MessageSerializer());
//    using var remoteConnection = new DriverRemoteConnection(client, "g");

    // Creating the graph traversal
    var g = Traversal().WithRemote(remoteConnection);
}

static async void BasicGremlinExample()
{
    var server = new GremlinServer("localhost", 8182);
    using var remoteConnection = new DriverRemoteConnection(new GremlinClient(server), "g");
    var g = Traversal().WithRemote(remoteConnection);

    // Basic Gremlin: adding and retrieving data
    var v1 = g.AddV("person").Property("name", "marko").Next();
    var v2 = g.AddV("person").Property("name", "stephen").Next();
    var v3 = g.AddV("person").Property("name", "vadas").Next();

    // Be sure to use a terminating step like next() or iterate() so that the traversal "executes"
    // Iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
    g.V(v1).AddE("knows").To(v2).Property("weight", 0.75).Iterate();
    g.V(v1).AddE("knows").To(v3).Property("weight", 0.75).Iterate();

    // Retrieve the data from the "marko" vertex
    var marko = await g.V().Has("person","name","marko").Values<string>("name").Promise(t => t.Next());
    Console.WriteLine("name: " + marko);
    
    // Find the "marko" vertex and then traverse to the people he "knows" and return their data
    var peopleMarkoKnows = await g.V().Has("person","name","marko").Out("knows").Values<string>("name").Promise(t => t.ToList());
    foreach (var person in peopleMarkoKnows)
    {
        Console.WriteLine("marko knows " + person);
    }
}

static void ModernTraversalExample()
{
    var server = new GremlinServer("localhost", 8182);
    using var remoteConnection = new DriverRemoteConnection(new GremlinClient(server), "g");
    var g = Traversal().WithRemote(remoteConnection);

    /*
    This example requires the Modern toy graph to be preloaded upon launching the Gremlin server.
    For details, see https://tinkerpop.apache.org/docs/current/reference/#gremlin-server-docker-image and use
    conf/gremlin-server-modern.yaml.
    */
    var e1 = g.V(1).BothE().ToList(); // (1)
    var e2 = g.V(1).BothE().Where(OtherV().HasId(2)).ToList(); // (2)
    var v1 = g.V(1).Next();
    var v2 = g.V(2).Next();
    var e3 = g.V(v1).BothE().Where(OtherV().Is(v2)).ToList(); // (3)
    var e4 = g.V(v1).OutE().Where(InV().Is(v2)).ToList(); // (4)
    var e5 = g.V(1).OutE().Where(InV().Has(T.Id, Within(2, 3))).ToList(); // (5)
    var e6 = g.V(1).Out().Where(__.In().HasId(6)).ToList(); // (6)

    Console.WriteLine(string.Join(", ", e1));
    Console.WriteLine(string.Join(", ", e2));
    Console.WriteLine(string.Join(", ", e3));
    Console.WriteLine(string.Join(", ", e4));
    Console.WriteLine(string.Join(", ", e5));
    Console.WriteLine(string.Join(", ", e6));

    /*
    1. There are three edges from the vertex with the identifier of "1".
    2. Filter those three edges using the Where()-step using the identifier of the vertex returned by OtherV() to
       ensure it matches on the vertex of concern, which is the one with an identifier of "2".
    3. Note that the same traversal will work if there are actual Vertex instances rather than just vertex
       identifiers.
    4. The vertex with identifier "1" has all outgoing edges, so it would also be acceptable to use the directional
       steps of OutE() and InV() since the schema allows it.
    5. There is also no problem with filtering the terminating side of the traversal on multiple vertices, in this
       case, vertices with identifiers "2" and "3".
    6. There’s no reason why the same pattern of exclusion used for edges with Where() can’t work for a vertex

       between two vertices.
    */
}