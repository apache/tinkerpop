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
using Gremlin.Net.Structure.IO.GraphBinary;
using Gremlin.Net.Structure.IO.GraphSON;
using static Gremlin.Net.Process.Traversal.AnonymousTraversalSource;
using static Gremlin.Net.Process.Traversal.__;

namespace Gremlin.Net.Benchmarks;

public class CompressionBenchmarks
{
    public static async Task GraphSONWithoutCompression()
    {
        var client = new GremlinClient(new GremlinServer("localhost", 45940), new GraphSON3MessageSerializer(),
            disableCompression: true);
        await PerformBenchmarkWithClient(client);
    }
    
    public static async Task GraphSONWithCompression()
    {
        var client = new GremlinClient(new GremlinServer("localhost", 45940), new GraphSON3MessageSerializer());
        await PerformBenchmarkWithClient(client);
    }
    
    public static async Task GraphBinaryWithoutCompression()
    {
        var client = new GremlinClient(new GremlinServer("localhost", 45940), new GraphBinaryMessageSerializer(),
            disableCompression: true);
        await PerformBenchmarkWithClient(client);
    }
    
    public static async Task GraphBinaryWithCompression()
    {
        var client = new GremlinClient(new GremlinServer("localhost", 45940), new GraphBinaryMessageSerializer());
        await PerformBenchmarkWithClient(client);
    }

    private static async Task PerformBenchmarkWithClient(GremlinClient client)
    {
        var g = Traversal().WithRemote(new DriverRemoteConnection(client));
        for (var i = 0; i < 5; i++)
        {
            await g.V().Repeat(Both()).Times(10).Emit().Fold().Promise(t => t.ToList());
        }
    }
}