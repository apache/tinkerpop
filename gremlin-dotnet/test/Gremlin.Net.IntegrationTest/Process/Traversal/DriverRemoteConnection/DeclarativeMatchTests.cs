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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System.Collections.Generic;
using System.Linq;
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    /// <summary>
    ///     Integration tests for the declarative <c>match(String)</c> step against the modern TinkerGraph.
    /// </summary>
    public class DeclarativeMatchTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        /// <summary>
        ///     Verifies that <c>g.Match("MATCH (p:person)-[e:knows]->(friend:person)")</c> returns
        ///     one binding <c>Map&lt;string, object&gt;</c> per result row — no <c>select()</c> required.
        ///     The modern graph has two <c>knows</c> edges out of marko: marko→vadas and marko→josh.
        /// </summary>
        [Fact]
        public void g_Match_PersonKnowsPerson_ReturnsBindingMaps()
        {
            var connection = _connectionFactory.CreateRemoteConnection("gmodern");
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var results = g.Match("MATCH (p:person)-[e:knows]->(friend:person)").ToList();

            Assert.Equal(2, results.Count);

            foreach (var result in results)
            {
                var row = (IDictionary<string, object>) result;
                Assert.True(row.ContainsKey("p"), "binding map must contain key 'p'");
                Assert.True(row.ContainsKey("friend"), "binding map must contain key 'friend'");
            }
        }
    }
}
