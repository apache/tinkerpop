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
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Docs.Reference
{
    public class GremlinApplicationsTests
    {
        [Fact(Skip="No Server under localhost")]
        public async Task ConnectingViaDriversTest()
        {
// tag::connectingViaDrivers[]
// script
var gremlinServer = new GremlinServer("localhost", 8182);
using (var gremlinClient = new GremlinClient(gremlinServer))
{
    var bindings = new Dictionary<string, object>
    {
        {"name", "marko"}
    };

    var response =
        await gremlinClient.SubmitWithSingleResultAsync<object>("g.V().has('person','name',name).out('knows')",
            bindings);
}

// bytecode
using (var gremlinClient = new GremlinClient(new GremlinServer("localhost", 8182)))
{
    var g = Traversal().WithRemote(new DriverRemoteConnection(gremlinClient));
    var list = g.V().Has("person", "name", "marko").Out("knows").ToList();
}
// end::connectingViaDrivers[]
        }
    }
}