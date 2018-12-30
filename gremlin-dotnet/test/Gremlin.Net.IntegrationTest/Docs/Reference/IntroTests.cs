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

using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
// tag::traversalSourceUsing[]
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
// end::traversalSourceUsing[]
using Xunit;
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;

namespace Gremlin.Net.IntegrationTest.Docs.Reference
{
    public class IntroTests
    {
        private readonly GraphTraversalSource g = Traversal()
            .WithRemote(new RemoteConnectionFactory().CreateRemoteConnection());

        [Fact(Skip="No Server under localhost")]
        public void TraversalSourceCreationTest()
        {
// tag::traversalSourceCreation[]
var g = Traversal().WithRemote(
    new DriverRemoteConnection(new GremlinClient(new GremlinServer("localhost", 8182))));
// end::traversalSourceCreation[]
        }
        
        [Fact(Skip="Graph manipulation would break other tests")]
        public void BasicGremlinAddsTest()
        {
// tag::basicGremlinAdds[]
var v1 = g.AddV("person").Property("name", "marko").Next();
var v2 = g.AddV("person").Property("name", "stephen").Next();
g.V(v1).AddE("knows").To(v2).Property("weight", 0.75).Iterate();
// end::basicGremlinAdds[]
        }

        [Fact]
        public void BasicGremlinMarkoKnowsTest()
        {
// tag::basicGremlinMarkoKnows[]
var marko = g.V().Has("person", "name", "marko").Next();
var peopleMarkoKnows = g.V().Has("person", "name", "marko").Out("knows").ToList();
// end::basicGremlinMarkoKnows[]

            Assert.Equal("person", marko.Label);
            Assert.Equal(2, peopleMarkoKnows.Count);
        }
    }
}                                                            