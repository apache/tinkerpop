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

using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure
{
    public class PropertyTests
    {
        [Fact]
        public void ShouldAssignPropertiesCorrectly()
        {
            const string key = "age";
            const int value = 29;
            var element = new Vertex(1);

            var property = new Property(key, value, element);

            Assert.Equal(key, property.Key);
            Assert.Equal(value, property.Value);
            Assert.Equal(element, property.Element);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsOfTwoEqualProperties()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            var secondProperty = new Property("age", 29, new Vertex(1));

            var areEqual = firstProperty.Equals(secondProperty);

            Assert.True(areEqual);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsWhereOtherIsNull()
        {
            var property = new Property("age", 29, new Vertex(1));

            var areEqual = property.Equals(null);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsOfPropertiesWithDifferentKeys()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            var secondProperty = new Property("aDifferentKey", 29, new Vertex(1));

            var areEqual = firstProperty.Equals(secondProperty);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsOfPropertiesWithDifferentValues()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            var secondProperty = new Property("age", 12, new Vertex(1));

            var areEqual = firstProperty.Equals(secondProperty);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsOfPropertiesWithDifferentElements()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            var secondProperty = new Property("age", 29, new Vertex(1234));

            var areEqual = firstProperty.Equals(secondProperty);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsObjectOfTwoEqualProperties()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            object secondProperty = new Property("age", 29, new Vertex(1));

            var areEqual = firstProperty.Equals(secondProperty);

            Assert.True(areEqual);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsObjectOfPropertiesWithDifferentKeys()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            object secondProperty = new Property("aDifferentKey", 29, new Vertex(1));

            var areEqual = firstProperty.Equals(secondProperty);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsObjectOfPropertiesWithDifferentValues()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            object secondProperty = new Property("age", 12, new Vertex(1));

            var areEqual = firstProperty.Equals(secondProperty);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsObjectOfPropertiesWithDifferentElements()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            object secondProperty = new Property("age", 29, new Vertex(1234));

            var areEqual = firstProperty.Equals(secondProperty);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnEqualHashcodesForEqualProperties()
        {
            var firstProperty = new Property("age", 29, new Vertex(1));
            var secondProperty = new Property("age", 29, new Vertex(1));

            var firstHashCode = firstProperty.GetHashCode();
            var secondHashCode = secondProperty.GetHashCode();

            Assert.Equal(firstHashCode, secondHashCode);
        }

        [Fact]
        public void ShouldReturnCommonStringRepresentationForToString()
        {
            var property = new Property("age", 29, new Vertex(1));

            var stringRepresentation = property.ToString();

            Assert.Equal("p[age->29]", stringRepresentation);
        }
    }
}