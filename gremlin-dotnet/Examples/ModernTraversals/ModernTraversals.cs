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

using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using static Gremlin.Net.Process.Traversal.__;
using static Gremlin.Net.Process.Traversal.P;

public class ModernTraversalExample
{
    static void Main()
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
}