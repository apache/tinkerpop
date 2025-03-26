﻿#region License

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
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    public class PredicateTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public void ShouldUsePredicatesCombinedWithPAndInHasStep()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var count = g.V().Has("age", P.Gt(30).And(P.Lt(35))).Count().Next();

            Assert.Equal(1, count);
        }

        [Fact]
        public void ShouldUsePWithinInHasStep()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().With(connection);

            var count = g.V().Has("name", P.Within("josh", "vadas")).Count().Next();

            Assert.Equal(2, count);
        }
        
        [Fact]
        public void ShouldUsePWithinWithListArgumentInHasStep()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().With(connection);
            var names = new List<string> {"josh", "vadas"};

            var count = g.V().Has("name", P.Within(names)).Count().Next();

            Assert.Equal(2, count);
        }
    }
}