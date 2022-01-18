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
using Gherkin.Ast;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;

using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    internal class ScenarioData : IDisposable
    {
        private static readonly string[] GraphNames = {"modern", "classic", "crew", "grateful", "sink"};

        private static readonly IDictionary<string, Vertex> EmptyVertices =
            new ReadOnlyDictionary<string, Vertex>(new Dictionary<string, Vertex>());
        
        private static readonly IDictionary<string, Edge> EmptyEdges =
            new ReadOnlyDictionary<string, Edge>(new Dictionary<string, Edge>());
        
        private static readonly IDictionary<string, VertexProperty> EmptyVertexProperties =
            new ReadOnlyDictionary<string, VertexProperty>(new Dictionary<string, VertexProperty>());
        
        private readonly RemoteConnectionFactory _connectionFactory;

        public Scenario CurrentScenario;
        public Feature CurrentFeature;

        public ScenarioDataPerGraph GetByGraphName(string name)
        {
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name), "Graph name can not be empty");
            }
            if (!_dataPerGraph.TryGetValue(name, out var data))
            {
                throw new KeyNotFoundException($"Graph data with key '{name}' not found");
            }
            return data;
        }

        public void CleanEmptyData()
        {
            var g = Traversal().WithRemote(GetByGraphName("empty").Connection);
            g.V().Drop().Iterate();
        }

        public void ReloadEmptyData()
        {
            var graphData = _dataPerGraph["empty"];
            var g = Traversal().WithRemote(graphData.Connection);
            graphData.Vertices = GetVertices(g);
            graphData.Edges = GetEdges(g);
            graphData.VertexProperties = GetVertexProperties(g);
        }

        private readonly IDictionary<string, ScenarioDataPerGraph> _dataPerGraph;
        
        public ScenarioData(IMessageSerializer messageSerializer)
        {
            _connectionFactory = new RemoteConnectionFactory(messageSerializer);
            _dataPerGraph = LoadDataPerGraph();
            var empty = new ScenarioDataPerGraph("empty", _connectionFactory.CreateRemoteConnection("ggraph"),
                new Dictionary<string, Vertex>(0), new Dictionary<string, Edge>(), new Dictionary<string, VertexProperty>());
            _dataPerGraph.Add("empty", empty);
        }

        private Dictionary<string, ScenarioDataPerGraph> LoadDataPerGraph()
        {
            return GraphNames.Select(name =>
            {
                var connection = _connectionFactory.CreateRemoteConnection($"g{name}");
                var g = Traversal().WithRemote(connection);
                return new ScenarioDataPerGraph(name, connection, GetVertices(g), GetEdges(g), GetVertexProperties(g));
            }).ToDictionary(x => x.Name);
        }

        private static IDictionary<string, Vertex> GetVertices(GraphTraversalSource g)
        {
            // Property name might not exist and C# doesn't support "null" keys in Dictionary
            if (g.V().Count().Next() == g.V().Has("name").Count().Next())
            {
                return g.V().Group<string, object>().By("name").By(__.Tail<Vertex>()).Next()
                    .ToDictionary(kv => kv.Key, kv => (Vertex) kv.Value);
            }
            else
            {
                return EmptyVertices;
            }
        }

        private static IDictionary<string, Edge> GetEdges(GraphTraversalSource g)
        {
            try
            {
                IFunction lambda = Lambda.Groovy(
                    "it.outVertex().value('name') + '-' + it.label() + '->' + it.inVertex().value('name')", 1);

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

        private static IDictionary<string, VertexProperty> GetVertexProperties(GraphTraversalSource g)
        {
            try
            {
                /*
                 * This closure will turn a VertexProperty into a triple string of the form:
                 * "vertexName-propKey->propVal"
                 *
                 * It will also take care of wrapping propVal in the appropriate Numeric format. We must do this in
                 * case a Vertex has multiple properties with the same key and number value but in different numeric
                 * type spaces (rare, but possible, and presumably something we may want to write tests around).
                 */
                var groovy = @"
                    { vp ->
                          def result = vp.element().value('name') + '-' + vp.key() + '->'
                          def value = vp.value()
                          def type = ''
                          switch(value) {
                            case { !(it instanceof Number) }:
                              return result + value
                            case Byte:
                              type = 'b'
                              break
                            case Short:
                              type = 's'
                              break
                            case Integer:
                              type = 'i'
                              break
                            case Long:
                              type = 'l'
                              break
                            case Float:
                              type = 'f'
                              break
                            case Double:
                              type = 'd'
                              break
                            case BigDecimal:
                              type = 'm'
                              break
                            case BigInteger:
                              type = 'n'
                              break
                          }
                          return result + 'd[' + value + '].' + type
                    }  
                ";
                
                IFunction lambda = Lambda.Groovy(groovy, 1);

                return g.V().Properties<VertexProperty>().Group<string, VertexProperty>()
                    .By(lambda)
                    .By(__.Tail<object>())
                    .Next();
            }
            catch (ResponseException)
            {
                return EmptyVertexProperties;
            }
        }

        public void Dispose()
        {
            _connectionFactory?.Dispose();
        }
    }

    internal class ScenarioDataPerGraph
    {
        public ScenarioDataPerGraph(string name, IRemoteConnection connection, IDictionary<string, Vertex> vertices,
                                    IDictionary<string, Edge> edges, IDictionary<string, VertexProperty> vertexProperties)
        {
            Name = name;
            Connection = connection;
            Vertices = vertices;
            Edges = edges;
            VertexProperties = vertexProperties;
        }

        public string Name { get; }
        
        public IRemoteConnection Connection { get;  }
        
        public IDictionary<string, Vertex> Vertices { get; set; }
        
        public IDictionary<string, Edge> Edges { get; set; }
        
        public IDictionary<string, VertexProperty> VertexProperties { get; set; }
    }
}