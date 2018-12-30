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
// tag::dslUsing[]
using Dsl;
using static Dsl.__Social;
// end::dslUsing[]
using Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection;
using Gremlin.Net.Process.Traversal;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Docs.Reference
{
    public class GremlinApplicationsDslTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact(Skip = "No Server under localhost")]
        public void DslTest()
        {
// tag::dslExamples[]
var connection = new DriverRemoteConnection(new GremlinClient(new GremlinServer("localhost", 8182)));
var social = Traversal().WithRemote(connection);

social.Persons("marko").Knows("josh");
social.Persons("marko").YoungestFriendsAge();
social.Persons().Filter(CreatedAtLeast(2)).Count();
// end::dslExamples[]
        }
        
        [Fact]
        public void ShouldUseDsl() 
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var social = AnonymousTraversalSource.Traversal().WithRemote(connection);

            Assert.NotNull(social.Persons("marko").Knows("josh").Next());
            Assert.Equal(27, social.Persons("marko").YoungestFriendsAge().Next());
            Assert.Equal(4, social.Persons().Count().Next());
            Assert.Equal(2, social.Persons("marko", "josh").Count().Next());
            Assert.Equal(1, social.Persons().Filter(__Social.CreatedAtLeast(2)).Count().Next());
        }
    }
}