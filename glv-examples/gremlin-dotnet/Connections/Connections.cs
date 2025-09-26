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
using Gremlin.Net.Structure.IO.GraphSON;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

public class ConnectionExample
{
    static void Main()
    {
        WithRemote();
        WithConf();
        WithSerializer();
    }

    // Connecting to the server
    static void WithRemote()
    {
        var server = new GremlinServer("localhost", 8182);
        using var remoteConnection = new DriverRemoteConnection(new GremlinClient(server), "g");
        var g = Traversal().WithRemote(remoteConnection);

        // Drop existing vertices
        g.V().Drop().Iterate();

        // Simple query to verify connection
        var v = g.AddV().Iterate();
        var count = g.V().Count().Next();
        Console.WriteLine("Vertex count: " + count);
    }

    // Connecting to the server with customized configurations
    static void WithConf()
    {
        using var remoteConnection = new DriverRemoteConnection(new GremlinClient(
        new GremlinServer(hostname: "localhost", port: 8182, enableSsl: false, username: "", password: "")), "g");
        var g = Traversal().WithRemote(remoteConnection);

        var v = g.AddV().Iterate();
        var count = g.V().Count().Next();
        Console.WriteLine("Vertex count: " + count);
    }

    // Specifying a serializer
    static void WithSerializer()
    {
        var server = new GremlinServer("localhost", 8182);
        var client = new GremlinClient(server, new GraphSON3MessageSerializer());
        using var remoteConnection = new DriverRemoteConnection(client, "g");
        var g = Traversal().WithRemote(remoteConnection);

        var v = g.AddV().Iterate();
        var count = g.V().Count().Next();
        Console.WriteLine("Vertex count: " + count);
    }
}
