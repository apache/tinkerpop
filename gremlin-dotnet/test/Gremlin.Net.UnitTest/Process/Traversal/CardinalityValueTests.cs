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

using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.UnitTest.Process.Traversal
{
    public class CardinalityValueTests
    {
        [Fact]
        public void ShouldProduceSingleValue()
        {
            CardinalityValue val = CardinalityValue.Single("test");
            Assert.Equal("test", val.Value);
            Assert.Equal(Cardinality.Single, val.Cardinality);
        }

        [Fact]
        public void ShouldProduceListValue()
        {
            CardinalityValue val = CardinalityValue.List("test");
            Assert.Equal("test", val.Value);
            Assert.Equal(Cardinality.List, val.Cardinality);
        }

        [Fact]
        public void ShouldProduceSetValue()
        {
            CardinalityValue val = CardinalityValue.Set("test");
            Assert.Equal("test", val.Value);
            Assert.Equal(Cardinality.Set, val.Cardinality);
        }
    }
}