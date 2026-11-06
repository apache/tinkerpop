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
    public class EdgeTests
    {
        [Fact]
        public void ShouldAssignPropertiesCorrectly()
        {
            const int id = 2;
            var outV = new Vertex(1);
            const string edgeLabel = "said";
            var inV = new Vertex("hello", "phrase");

            var edge = new Edge(id, outV, edgeLabel, inV);

            Assert.Equal(outV, edge.OutV);
            Assert.Equal(inV, edge.InV);
            Assert.Equal(edgeLabel, edge.Label);
            Assert.NotEqual(edge.InV, edge.OutV);
        }

        [Fact]
        public void ShouldReturnCommonStringRepresentationForToString()
        {
            var edge = new Edge(2, new Vertex(1), "said", new Vertex("hello", "phrase"));

            var edgeStr = edge.ToString();

            Assert.Equal("e[2][1-said->hello]", edgeStr);
        }

        [Fact]
        public void ShouldGroupMultiPropertiesUnderOneKeyForPropertyMap()
        {
            var first = new Property("name", "marko");
            var second = new Property("name", "marko a. rodriguez");
            var edge = new Edge(2, new Vertex(1), "said", new Vertex("hello", "phrase"),
                new dynamic[] {first, second});

            var propertyMap = edge.PropertyMap();

            Assert.Single(propertyMap);
            Assert.True(propertyMap.ContainsKey("name"));
            Assert.Equal(new List<Property> {first, second}, propertyMap["name"]);
        }

        [Fact]
        public void ShouldGroupSingleValuedPropertiesIntoSingleElementListsForPropertyMap()
        {
            var since = new Property("since", 2009);
            var weight = new Property("weight", 0.5);
            var edge = new Edge(2, new Vertex(1), "said", new Vertex("hello", "phrase"),
                new dynamic[] {since, weight});

            var propertyMap = edge.PropertyMap();

            Assert.Equal(2, propertyMap.Count);
            Assert.Equal(new List<Property> {since}, propertyMap["since"]);
            Assert.Equal(new List<Property> {weight}, propertyMap["weight"]);
        }

        [Fact]
        public void ShouldReturnEmptyDictionaryForPropertyMapWhenNoProperties()
        {
            var edge = new Edge(2, new Vertex(1), "said", new Vertex("hello", "phrase"));

            var propertyMap = edge.PropertyMap();

            Assert.Empty(propertyMap);
        }
    }
}
