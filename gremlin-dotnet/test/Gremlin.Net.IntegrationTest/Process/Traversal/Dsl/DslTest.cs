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

using System.Collections.Generic;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.Dsl {

    public static class SocialTraversal {
        public static GraphTraversal<Vertex,Vertex> knows(this GraphTraversal<Vertex,Vertex> t, string personName) {
            return t.Out("knows").HasLabel("person").Has("name", personName);
        }
    }

    public static class SocialTraversalSource {
        public static GraphTraversal<Vertex,Vertex> persons(this GraphTraversalSource g, params string[] personNames) {
            GraphTraversal<Vertex,Vertex> t = g.V().HasLabel("person");

            if (personNames.Length > 0) {    
                t = t.Has("name", P.Within(personNames));
            }

            return t;
        }
    }

    public class DslTest {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();
        
        [Fact]
        public void ShouldUseDsl() {
            var graph = new Graph();
            var connection = _connectionFactory.CreateRemoteConnection();
            var social = graph.Traversal().WithRemote(connection);

            Assert.NotNull(social.persons("marko").knows("josh").Next());
        }
    }
}