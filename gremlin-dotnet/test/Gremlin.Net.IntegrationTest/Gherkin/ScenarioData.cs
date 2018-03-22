#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    internal class ScenarioData
    {
        private static readonly Lazy<ScenarioData> Lazy = new Lazy<ScenarioData>(Load);
        
        private static readonly string[] GraphNames = {"modern", "classic", "crew", "grateful", "sink"};

        private static readonly IDictionary<string, Vertex> EmptyVertices =
            new ReadOnlyDictionary<string, Vertex>(new Dictionary<string, Vertex>());
        
        private static readonly IDictionary<string, Edge> EmptyEdges =
            new ReadOnlyDictionary<string, Edge>(new Dictionary<string, Edge>());
        
        private static readonly RemoteConnectionFactory ConnectionFactory = new RemoteConnectionFactory();

        public static ScenarioDataPerGraph GetByGraphName(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name), "Graph name can not be empty");
            }
            var dataPerGraph = Lazy.Value._dataPerGraph;
            if (!dataPerGraph.TryGetValue(name, out var data))
            {
                throw new KeyNotFoundException($"Graph data with key '{name}' not found");
            }
            return data;
        }

        public static void Shutdown()
        {
            ConnectionFactory.Dispose();
        }

        public static void CleanEmptyData()
        {
            var g = new Graph().Traversal().WithRemote(GetByGraphName("empty").Connection);
            g.V().Drop().Iterate();
        }

        public static void ReloadEmptyData()
        {
            var graphData = Lazy.Value._dataPerGraph["empty"];
            var g = new Graph().Traversal().WithRemote(graphData.Connection);
            graphData.Vertices = GetVertices(g);
            graphData.Edges = GetEdges(g);
        }

        private readonly IDictionary<string, ScenarioDataPerGraph> _dataPerGraph;
        
        private ScenarioData(IDictionary<string, ScenarioDataPerGraph> dataPerGraph)
        {
            _dataPerGraph = new Dictionary<string, ScenarioDataPerGraph>(dataPerGraph);
            var empty = new ScenarioDataPerGraph("empty", ConnectionFactory.CreateRemoteConnection("ggraph"),
                new Dictionary<string, Vertex>(0), new Dictionary<string, Edge>());
            _dataPerGraph.Add("empty", empty);
        }

        private static ScenarioData Load()
        {
            return new ScenarioData(GraphNames.Select(name =>
            {
                var connection = ConnectionFactory.CreateRemoteConnection($"g{name}");
                var g = new Graph().Traversal().WithRemote(connection);
                return new ScenarioDataPerGraph(name, connection, GetVertices(g), GetEdges(g));
            }).ToDictionary(x => x.Name));
        }

        private static IDictionary<string, Vertex> GetVertices(GraphTraversalSource g)
        {
            try
            {
                return g.V().Group<string, object>().By("name").By(__.Tail<Vertex>()).Next()
                    .ToDictionary(kv => kv.Key, kv => (Vertex) kv.Value);
            }
            catch (ResponseException)
            {
                // Property name might not exist
                return EmptyVertices;
            }
        }

        private static IDictionary<string, Edge> GetEdges(GraphTraversalSource g)
        {
            try
            {
                IFunction lambda = Lambda.Groovy(
                    "it.outVertex().value('name') + '-' + it.label() + '->' + it.inVertex().value('name')");

                return g.E().Group<string, Edge>()
                    .By(lambda)
                    .By(__.Tail<object>())
                    .Next();
            }
            catch (ResponseException)
            {
                // Property name might not exist
                return EmptyEdges;
            }
        }
    }

    internal class ScenarioDataPerGraph
    {
        public ScenarioDataPerGraph(string name, IRemoteConnection connection, IDictionary<string, Vertex> vertices,
                                    IDictionary<string, Edge> edges)
        {
            Name = name;
            Connection = connection;
            Vertices = vertices;
            Edges = edges;
        }

        public string Name { get; }
        
        public IRemoteConnection Connection { get;  }
        
        public IDictionary<string, Vertex> Vertices { get; set; }
        
        public IDictionary<string, Edge> Edges { get; set; }
    }
}