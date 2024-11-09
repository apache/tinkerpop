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
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.IntegrationTest;
using Xunit;

using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;

namespace Gremlin.Net.Template.IntegrationTest
{
    public class ServiceTests
    {
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"]!;
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);
        private const string TestTraversalSource = "gmodern";

        [Fact]
        public void ShouldReturnExpectedCreators()
        {
            using (var client = CreateClient())
            {
                var g = Traversal().WithRemote(new DriverRemoteConnection(client, TestTraversalSource));
                var service = new Service(g);
            
                var creators = service.FindCreatorsOfSoftware("lop");

                Assert.Equal(new List<string?> {"marko", "josh", "peter"}, creators);
            }
        }

        private static IGremlinClient CreateClient()
        {
            return new GremlinClient(new GremlinServer(TestHost, TestPort));
        }
    }
}
