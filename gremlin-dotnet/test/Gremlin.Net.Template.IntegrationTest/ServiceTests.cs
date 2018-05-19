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
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.Template.IntegrationTest
{
    public class ServiceTests
    {
        private const string TestHost = "localhost";
        private const int TestPort = 45940;

        [Fact]
        public void ShouldReturnExpectedCreators()
        {
            using (var client = CreateClient())
            {
                var g = new Graph().Traversal().WithRemote(new DriverRemoteConnection(client));
                var service = new Service(g);
            
                var creators = service.FindCreatorsOfSoftware("lop");

                Assert.Equal(new List<string> {"marko", "josh", "peter"}, creators);
            }
        }

        private static IGremlinClient CreateClient()
        {
            return new GremlinClient(new GremlinServer(TestHost, TestPort));
        }
    }
}
