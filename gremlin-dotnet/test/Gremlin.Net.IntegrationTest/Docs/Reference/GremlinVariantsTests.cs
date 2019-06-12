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

using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Process.Traversal.Step.Util;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;
// tag::commonImports[]
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
using static Gremlin.Net.Process.Traversal.T;
// end::commonImports[]

namespace Gremlin.Net.IntegrationTest.Docs.Reference
{
    public class GremlinVariantsTests
    {
        private readonly GraphTraversalSource g = Traversal()
            .WithRemote(new RemoteConnectionFactory().CreateRemoteConnection());
        
        [Fact(Skip="No Server under localhost")]
        public void ConnectingTest()
        {
// tag::connecting[]
var remoteConnection = new DriverRemoteConnection(new GremlinClient(new GremlinServer("localhost", 8182)));
var g = Traversal().WithRemote(remoteConnection);
// end::connecting[]
        }
        
        [Fact(Skip="No Server under localhost")]
        public void SerializationTest()
        {
// tag::serialization[]
var client = new GremlinClient(new GremlinServer("localhost", 8182), new GraphSON2Reader(),
    new GraphSON2Writer(), GremlinClient.GraphSON2MimeType);
// end::serialization[]
        }
        
        [Fact(Skip="We can't apply strategies")]
        public void TraversalStrategiesTest()
        {
            var g = this.g;
// tag::traversalStrategies[]
g = g.WithStrategies(new SubgraphStrategy(vertices: HasLabel("person"),
    edges: Has("weight", Gt(0.5))));
var names = g.V().Values<string>("name").ToList();  // names: [marko, vadas, josh, peter]

g = g.WithoutStrategies(typeof(SubgraphStrategy));
names = g.V().Values<string>("name").ToList(); // names: [marko, vadas, lop, josh, ripple, peter]

var edgeValueMaps = g.V().OutE().ValueMap<object, object>().With(WithOptions.Tokens).ToList();
// edgeValueMaps: [[label:created, id:9, weight:0.4], [label:knows, id:7, weight:0.5], [label:knows, id:8, weight:1.0],
//     [label:created, id:10, weight:1.0], [label:created, id:11, weight:0.4], [label:created, id:12, weight:0.2]]

g = g.WithComputer(workers: 2, vertices: Has("name", "marko"));
names = g.V().Values<string>("name").ToList();  // names: [marko]

edgeValueMaps = g.V().OutE().ValueMap<object, object>().With(WithOptions.Tokens).ToList();
// edgeValueMaps: [[label:created, id:9, weight:0.4], [label:knows, id:7, weight:0.5], [label:knows, id:8, weight:1.0]]
// end::traversalStrategies[]
        }
        
        [Fact(Skip="No Server under localhost")]
        public async Task SubmittingScriptsTest()
        {
// tag::submittingScripts[]
var gremlinServer = new GremlinServer("localhost", 8182);
using (var gremlinClient = new GremlinClient(gremlinServer))
{
    var response =
        await gremlinClient.SubmitWithSingleResultAsync<string>("g.V().has('person','name','marko')");
}
// end::submittingScripts[]
        }

        [Fact(Skip = "No Server under localhost")]
        public void SubmittingScriptsWithAuthenticationTest()
        {
// tag::submittingScriptsWithAuthentication[]
var username = "username";
var password = "password";
var gremlinServer = new GremlinServer("localhost", 8182, true, username, password);
// end::submittingScriptsWithAuthentication[]
        }
    }
}