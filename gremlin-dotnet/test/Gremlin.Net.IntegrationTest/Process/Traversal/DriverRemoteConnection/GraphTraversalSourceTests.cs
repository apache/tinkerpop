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
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Process.Traversal.DriverRemoteConnection
{
    public class GraphTraversalSourceTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public void ShouldUseSideEffectSpecifiedInWithSideEffect()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            var results = g.WithSideEffect("a", new List<string> {"josh", "peter"})
                .V(1)
                .Out("created")
                .In("created")
                .Values<string>("name")
                .Where(P.Within("a"))
                .ToList();

            Assert.Equal(2, results.Count);
            Assert.Contains("josh", results);
            Assert.Contains("peter", results);
        }
                
        [Fact]
        public void ShouldHandleLambdasInWithSack()
        {
            var connection = _connectionFactory.CreateRemoteConnection();
            var g = AnonymousTraversalSource.Traversal().WithRemote(connection);

            Assert.Equal(24.0, g.WithSack(1.0, (IUnaryOperator) Lambda.Groovy("x -> x + 1")).V().Both().Sack<double>().Sum<double>().Next());                        
            Assert.Equal(24.0, g.WithSack((ISupplier) Lambda.Groovy("{1.0d}"), (IUnaryOperator) Lambda.Groovy("x -> x + 1")).V().Both().Sack<double>().Sum<double>().Next());
            Assert.Equal(48.0, g.WithSack(1.0, (IBinaryOperator) Lambda.Groovy("x, y -> x + y + 1")).V().Both().Sack<double>().Sum<double>().Next());                        
            Assert.Equal(48.0, g.WithSack((ISupplier) Lambda.Groovy("{1.0d}"), (IBinaryOperator) Lambda.Groovy("x, y -> x + y + 1")).V().Both().Sack<double>().Sum<double>().Next());       
        }
    }
}