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
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Structure;

namespace Gremlin.Net.Template
{
    internal class Program
    {
        private const string GremlinServerHostname = "localhost";
        private const int GremlinServerPort = 45940;

        private static void Main()
        {
            using (var client = new GremlinClient(new GremlinServer(GremlinServerHostname, GremlinServerPort)))
            {
                var g = new Graph().Traversal().WithRemote(new DriverRemoteConnection(client));
                var service = new Service(g);
                var creators = service.FindCreatorsOfSoftware("lop");
                foreach (var c in creators)
                {
                    Console.WriteLine(c);
                }
            }
        }
    }
}
