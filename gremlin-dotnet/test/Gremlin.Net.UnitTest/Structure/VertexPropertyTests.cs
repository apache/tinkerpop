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
    public class VertexPropertyTests
    {
        [Fact]
        public void ShouldAssignPropertiesCorrectly()
        {
            const long id = 24;
            const string label = "name";
            const string value = "marko";
            var vertex = new Vertex(1);

            var vertexProperty = new VertexProperty(id, label, value, vertex);

            Assert.Equal(label, vertexProperty.Label);
            Assert.Equal(label, vertexProperty.Key);
            Assert.Equal(value, vertexProperty.Value);
            Assert.Equal(id, vertexProperty.Id);
            Assert.Equal(vertex, vertexProperty.Vertex);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsOfTwoEqualVertexProperties()
        {
            var firstVertexProperty = new VertexProperty((long) 24, "name", "marko", new Vertex(1));
            var secondVertexProperty = new VertexProperty((long) 24, "name", "marko", new Vertex(1));

            var areEqual = firstVertexProperty.Equals(secondVertexProperty);

            Assert.True(areEqual);
        }

        [Fact]
        public void ShouldReturnCommonStringRepresentationForToString()
        {
            var vertexProperty = new VertexProperty((long) 24, "name", "marko", new Vertex(1));

            var stringRepresentation = vertexProperty.ToString();

            Assert.Equal("vp[name->marko]", stringRepresentation);
        }
    }
}