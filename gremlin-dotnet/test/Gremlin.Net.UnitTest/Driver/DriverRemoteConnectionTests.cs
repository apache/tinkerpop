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
using NSubstitute;
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class DriverRemoteConnectionTests
    {
        [Fact]
        public void ShouldDisposeProvidedGremlinClientOnDispose()
        {
            var gremlinClient = Substitute.For<IGremlinClient>();
            var driverRemoteConnection = new DriverRemoteConnection(gremlinClient);

            driverRemoteConnection.Dispose();

            gremlinClient.Received().Dispose();
        }

        [Fact]
        public void ShouldThrowWhenGivenNullAsGremlinClient()
        {
            Assert.Throws<ArgumentNullException>(() => new DriverRemoteConnection(null!));
        }
    }
}