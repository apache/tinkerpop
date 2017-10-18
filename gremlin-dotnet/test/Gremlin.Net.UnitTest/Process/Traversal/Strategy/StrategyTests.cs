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
using System.Linq;
using System.Reflection;
using Gremlin.Net.Process.Traversal.Strategy;
using Gremlin.Net.Process.Traversal.Strategy.Optimization;
using Gremlin.Net.Process.Traversal.Strategy.Verification;
using Xunit;

namespace Gremlin.Net.UnitTest.Process.Traversal.Strategy
{
    public class StrategyTests
    {
        [Fact]
        public void ShouldReturnFalseForEqualsOfStrategiesWithDifferentStrategyNames()
        {
            var firstStrategy = new TestStrategy("aConfigKey", "aConfigValue");
            var secondStrategy = new IncidentToAdjacentStrategy();

            var areEqual = firstStrategy.Equals(secondStrategy);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsOfStrategiesWithEqualNamesButDifferentConfigurations()
        {
            var firstStrategy = new TestStrategy("aConfigKey", "aConfigValue");
            var secondStrategy = new TestStrategy("anotherKey", "anotherValue");

            var areEqual = firstStrategy.Equals(secondStrategy);

            Assert.True(areEqual);
        }

        [Fact]
        public void ShouldReturnDifferentHashcodesForStrategiesWithDifferentNames()
        {
            var firstStrategy = new TestStrategy();
            var secondStrategy = new ReadOnlyStrategy();

            var firstHashCode = firstStrategy.GetHashCode();
            var secondHashCode = secondStrategy.GetHashCode();

            Assert.NotEqual(firstHashCode, secondHashCode);
        }

        [Fact]
        public void ShouldReturnEqualHashcodesForStrategiesWithEqualNamesButDifferentConfigurations()
        {
            var firstStrategy = new TestStrategy("aConfigKey", "aConfigValue");
            var secondStrategy = new TestStrategy("anotherKey", "anotherValue");

            var firstHashCode = firstStrategy.GetHashCode();
            var secondHashCode = secondStrategy.GetHashCode();

            Assert.Equal(firstHashCode, secondHashCode);
        }

        [Fact]
        public void ShouldReturnClassNameForStrategyNameProperty()
        {
            var testStrategy = new TestStrategy();

            Assert.Equal("TestStrategy", testStrategy.StrategyName);
        }

        [Fact]
        public void ShouldReturnStrategyNameWhenForToString()
        {
            var testStrategy = new TestStrategy();

            var strategyStr = testStrategy.ToString();

            Assert.Equal("TestStrategy", strategyStr);
        }

        [Fact]
        public void AllStrategiesShouldHaveADefaultConstructor()
        {
            // We need a default constructor as the ClassWriter needs that for serialization
            foreach (var type in _allStrategyTypes)
            {
                Assert.True(HasParameterlessConstructor(type), $"{type} has no parameterless constructor");
            }
        }

        private readonly IEnumerable<Type> _allStrategyTypes = typeof(AbstractTraversalStrategy).GetTypeInfo().Assembly
            .GetTypes().Where(t => typeof(AbstractTraversalStrategy).IsAssignableFrom(t))
            .Where(t => t != typeof(AbstractTraversalStrategy));

        private bool HasParameterlessConstructor(Type type)
        {
            try
            {
                Activator.CreateInstance(type);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }

    internal class TestStrategy : AbstractTraversalStrategy
    {
        public TestStrategy()
        {
        }

        public TestStrategy(string configKey, dynamic configValue)
        {
            Configuration[configKey] = configValue;
        }
    }
}