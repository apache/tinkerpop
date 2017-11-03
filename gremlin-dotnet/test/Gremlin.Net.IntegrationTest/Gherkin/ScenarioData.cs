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
using System.Linq;
using System.Text.RegularExpressions;
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;

namespace Gremlin.Net.IntegrationTest.Gherkin
{
    public class ScenarioData
    {
        private static readonly Lazy<ScenarioData> Lazy = new Lazy<ScenarioData>(Load);
        
        public static ScenarioData Instance => Lazy.Value;
        
        private static readonly Regex EdgeORegex = new Regex("o=(.+?)[,}]", RegexOptions.Compiled);
        private static readonly Regex EdgeLRegex = new Regex("l=(.+?)[,}]", RegexOptions.Compiled);
        private static readonly Regex EdgeIRegex = new Regex("i=(.+?)[,}]", RegexOptions.Compiled);

        public IDictionary<string, Vertex> ModernVertices { get; }
        
        public IDictionary<string, Edge> ModernEdges { get; }
        
        private ScenarioData(IDictionary<string, Vertex> modernVertices, IDictionary<string, Edge> modernEdges)
        {
            ModernVertices = modernVertices;
            ModernEdges = modernEdges;
        }

        private static ScenarioData Load()
        {
            var connectionFactory = new RemoteConnectionFactory();
            var g = new Graph().Traversal().WithRemote(connectionFactory.CreateRemoteConnection());
            var vertices = g.V().Group<string, object>().By("name").By(__.Tail<Vertex>()).Next()
                .ToDictionary(kv => kv.Key, kv => (Vertex)kv.Value);
            var edges = g.E().Group<string, object>()
                .By(__.Project<Edge>("o", "l", "i")
                    .By(__.OutV().Values<string>("name")).By(__.Label()).By(__.InV().Values<string>("name")))
                .By(__.Tail<object>())
                .Next()
                .ToDictionary(kv => GetEdgeKey(kv.Key), kv => (Edge)kv.Value);
            connectionFactory.Dispose();
            return new ScenarioData(vertices, edges);
        }

        private static string GetEdgeKey(string key)
        {
            var o = EdgeORegex.Match(key).Groups[1].Value;
            var l = EdgeLRegex.Match(key).Groups[1].Value;
            var i = EdgeIRegex.Match(key).Groups[1].Value;
            return o + "-" + l + "->" + i;
        }
    }
}