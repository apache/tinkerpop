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
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

public class ConnectionExample
{
    static readonly string ServerHost = Environment.GetEnvironmentVariable("GREMLIN_SERVER_HOST") ?? "localhost";
    static readonly int ServerPort = int.Parse(Environment.GetEnvironmentVariable("GREMLIN_SERVER_PORT") ?? "8182");
    static readonly int SecureServerPort = int.Parse(Environment.GetEnvironmentVariable("GREMLIN_SECURE_SERVER_PORT") ?? "8183");
    static readonly string VertexLabel = Environment.GetEnvironmentVariable("VERTEX_LABEL") ?? "connection";

    static void Main()
    {
        WithRemoteConnection();
        WithConf();
        WithBasicAuth();
    }

    // Connecting to the server
    static void WithRemoteConnection()
    {
        var server = new GremlinServer(ServerHost, ServerPort);
        using var remoteConnection = new DriverRemoteConnection(new GremlinClient(server), "g");
        var g = Traversal().With(remoteConnection);

        // Simple query to verify connection
        var v = g.AddV(VertexLabel).Iterate();
        var count = g.V().HasLabel(VertexLabel).Count().Next();
        Console.WriteLine("Vertex count: " + count);
    }

    // Connecting to the server with customized connection settings
    static void WithConf()
    {
        var server = new GremlinServer(ServerHost, ServerPort);
        var settings = new ConnectionSettings
        {
            ConnectionTimeout = TimeSpan.FromSeconds(30),
        };
        using var remoteConnection = new DriverRemoteConnection(
            new GremlinClient(server, connectionSettings: settings), "g");
        var g = Traversal().With(remoteConnection);

        var v = g.AddV(VertexLabel).Iterate();
        var count = g.V().HasLabel(VertexLabel).Count().Next();
        Console.WriteLine("Vertex count: " + count);
    }

    // Connecting with basic authentication using a request interceptor
    static void WithBasicAuth()
    {
        var server = new GremlinServer(ServerHost, SecureServerPort, enableSsl: true);
        var client = new GremlinClient(server,
            connectionSettings: new ConnectionSettings { SkipCertificateValidation = true },
            interceptors: new[] { Auth.BasicAuth("stephen", "password") });
        using var remoteConnection = new DriverRemoteConnection(client, "g");
        var g = Traversal().With(remoteConnection);

        var v = g.AddV(VertexLabel).Iterate();
        var count = g.V().HasLabel(VertexLabel).Count().Next();
        Console.WriteLine("Vertex count: " + count);
    }
}
