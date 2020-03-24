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
using Xunit;

namespace Gremlin.Net.UnitTest.Driver
{
    public class ConnectionPoolSettingsTests
    {
        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(-100)]
        public void ShouldThrowForInvalidPoolSize(int invalidPoolSize)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ConnectionPoolSettings {PoolSize = invalidPoolSize});
        }
        
        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        [InlineData(-100)]
        public void ShouldThrowForInvalidMaxInProcessPerConnection(int invalidMaxInProcessPerConnection)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ConnectionPoolSettings
                {MaxInProcessPerConnection = invalidMaxInProcessPerConnection});
        }
        
        [Theory]
        [InlineData(-1)]
        [InlineData(-100)]
        public void ShouldThrowForInvalidReconnectionAttempts(int reconnectionAttempts)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ConnectionPoolSettings
                {ReconnectionAttempts = reconnectionAttempts});
        }

        [Theory]
        [InlineData(-1)]
        [InlineData(-1000)]
        [InlineData(0)]
        public void ShouldThrowForInvalidReconnectionBaseDelay(int baseDelayInMs)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new ConnectionPoolSettings
                {ReconnectionBaseDelay = TimeSpan.FromMilliseconds(baseDelayInMs)});
        }
    }
}