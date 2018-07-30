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
using Gremlin.Net.Process.Remote;
using DriverRemoteConnectionImpl = Gremlin.Net.Driver.Remote.DriverRemoteConnection;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    internal class RemoteConnectionFactory : IDisposable
    {
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);

        private readonly IList<DriverRemoteConnectionImpl> _connections = new List<DriverRemoteConnectionImpl>();

        public IRemoteConnection CreateRemoteConnection()
        {
            // gmodern is the standard test traversalsource that the main body of test uses
            return CreateRemoteConnection("gmodern");
        }

        public IRemoteConnection CreateRemoteConnection(string traversalSource)
        {
            var c = new DriverRemoteConnectionImpl(
                new GremlinClient(new GremlinServer(TestHost, TestPort),
                    connectionPoolSettings: new ConnectionPoolSettings {MinSize = 1}),
                traversalSource);
            _connections.Add(c);
            return c;
        }

        public void Dispose()
        {
            foreach (var connection in _connections)
            {
                connection.Dispose();
            }
        }
    }
}