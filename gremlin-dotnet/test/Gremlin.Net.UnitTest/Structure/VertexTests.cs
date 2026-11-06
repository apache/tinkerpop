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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure
{
    public class VertexTests
    {
        [Fact]
        public void ShouldReturnCommonStringRepresentationForToString()
        {
            var vertex = new Vertex(12345);

            var vertexString = vertex.ToString();

            Assert.Equal("v[12345]", vertexString);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsOfTwoEqualVertices()
        {
            var firstVertex = new Vertex(1);
            var secondVertex = new Vertex(1);

            var areEqual = firstVertex.Equals(secondVertex);

            Assert.True(areEqual);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsWhereOtherIsNull()
        {
            var vertex = new Vertex(1);

            var areEqual = vertex.Equals(null);

            Assert.False(areEqual);
        }

        [Fact]
        public void ShouldReturnSpecifiedLabelForLabelProperty()
        {
            const string specifiedLabel = "person";

            var vertex = new Vertex(1, specifiedLabel);

            Assert.Equal(specifiedLabel, vertex.Label);
        }

        [Fact]
        public void ShouldReturnDefaultLabelForLabelPropertyWhenNoLabelSpecified()
        {
            var vertex = new Vertex(1);

            Assert.Equal("vertex", vertex.Label);
        }

        [Fact]
        public void ShouldGroupMultiPropertiesUnderOneKeyForPropertyMap()
        {
            var vertex = new Vertex(1);
            var first = new VertexProperty((long) 0, "name", "marko", vertex);
            var second = new VertexProperty((long) 1, "name", "marko a. rodriguez", vertex);
            var vertexWithProperties = new Vertex(1, "person", new dynamic[] {first, second});

            var propertyMap = vertexWithProperties.PropertyMap();

            Assert.Single(propertyMap);
            Assert.True(propertyMap.ContainsKey("name"));
            Assert.Equal(new List<VertexProperty> {first, second}, propertyMap["name"]);
        }

        [Fact]
        public void ShouldGroupSingleValuedPropertiesIntoSingleElementListsForPropertyMap()
        {
            var vertex = new Vertex(1);
            var name = new VertexProperty((long) 0, "name", "marko", vertex);
            var age = new VertexProperty((long) 1, "age", 29, vertex);
            var vertexWithProperties = new Vertex(1, "person", new dynamic[] {name, age});

            var propertyMap = vertexWithProperties.PropertyMap();

            Assert.Equal(2, propertyMap.Count);
            Assert.Equal(new List<VertexProperty> {name}, propertyMap["name"]);
            Assert.Equal(new List<VertexProperty> {age}, propertyMap["age"]);
        }

        [Fact]
        public void ShouldReturnEmptyDictionaryForPropertyMapWhenNoProperties()
        {
            var vertex = new Vertex(1);

            var propertyMap = vertex.PropertyMap();

            Assert.Empty(propertyMap);
        }
    }
}