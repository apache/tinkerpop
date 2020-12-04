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
using Gremlin.Net.Structure.IO;
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class GremlinClientTests
    {
        [Fact]
        public void ShouldThrowOnExecutionOfMultiConnectionInPool()
        {
            var host = "localhost";
            var port = 8182;
            var sessionId = Guid.NewGuid().ToString();
            // set pool size more than 1
            var poolSettings = new ConnectionPoolSettings {PoolSize = 2};

            var gremlinServer = new GremlinServer(host, port);
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new GremlinClient(gremlinServer, connectionPoolSettings: poolSettings, sessionId: sessionId));
        }

#pragma warning disable 612,618
        [Fact]
        public void ShouldThrowForInvalidGraphSONReaderWriterCombination()
        {
            Assert.Throws<ArgumentException>(() =>
                new GremlinClient(new GremlinServer(), new GraphSON2Reader(), new GraphSON3Writer()));
        }
        
        [Fact]
        public void ShouldThrowForInvalidGraphSONReaderForGivenMimeType()
        {
            Assert.Throws<ArgumentException>(() =>
                new GremlinClient(new GremlinServer(), new GraphSON3Reader(), new GraphSON2Writer(),
                    SerializationTokens.GraphSON2MimeType));
        }
        
        [Fact]
        public void ShouldThrowForInvalidGraphSONWriterForGivenMimeType()
        {
            Assert.Throws<ArgumentException>(() =>
                new GremlinClient(new GremlinServer(), new GraphSON2Reader(), new GraphSON3Writer(),
                    SerializationTokens.GraphSON2MimeType));
        }

        [Fact]
        public void ShouldThrowForUnsupportedMimeType()
        {
            Assert.Throws<ArgumentException>(() =>
                new GremlinClient(new GremlinServer(), new GraphSON3Reader(), new GraphSON3Writer(), "unsupported"));
        }
#pragma warning restore 612,618
    }
}