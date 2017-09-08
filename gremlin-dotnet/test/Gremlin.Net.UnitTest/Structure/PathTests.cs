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
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure
{
    public class PathTests
    {
        [Fact]
        public void ShouldAssignPropertiesCorrectly()
        {
            var labels = new List<ISet<string>>
            {
                new HashSet<string> {"a", "b"},
                new HashSet<string> {"c", "b"},
                new HashSet<string>()
            };
            var objects = new List<object> {1, new Vertex(1), "hello"};

            var path = new Path(labels, objects);

            Assert.Equal(labels, path.Labels);
            Assert.Equal(objects, path.Objects);
        }

        [Fact]
        public void ShouldReturnTrueForContainsKeyWhenGivenKeyExists()
        {
            var labels = new List<ISet<string>>
            {
                new HashSet<string> {"a", "b"},
                new HashSet<string> {"c", "b"},
                new HashSet<string>()
            };
            var path = new Path(labels, new List<object>());

            var containsKey = path.ContainsKey("c");

            Assert.True(containsKey);
        }

        [Fact]
        public void ShouldReturnFalseForContainsKeyWhenGivenKeyDoesNotExist()
        {
        var labels = new List<ISet<string>>
            {
                new HashSet<string> {"a", "b"},
                new HashSet<string> {"c", "b"},
                new HashSet<string>()
            };
            var path = new Path(labels, new List<object>());

            var containsKey = path.ContainsKey("z");

            Assert.False(containsKey);
        }

        [Fact]
        public void ShouldReturnCountOfObjectsForCountProperty()
        {
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(new List<ISet<string>>(), objects);

            var count = path.Count;

            Assert.Equal(3, count);
        }

        [Fact]
        public void ShouldEnumeratorObjectsIntoListWhenToListIsCalled()
        {
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(new List<ISet<string>>(), objects);

            var enumeratedObj = path.ToList();

            Assert.Equal(objects, enumeratedObj);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsOfTwoEqualPaths()
        {
            var firstPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> {1, new Vertex(1), "hello"});
            var secondPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> {1, new Vertex(1), "hello"});

            var equals = firstPath.Equals(secondPath);

            Assert.True(equals);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsOfPathsWithDifferentLabels()
        {
            var firstPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> {1, new Vertex(1), "hello"});
            var secondPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> {1, new Vertex(1), "hello"});

            var equals = firstPath.Equals(secondPath);

            Assert.False(equals);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsOfPathsWithDifferentObjects()
        {
            var firstPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> {1, new Vertex(1), "hello"});
            var secondPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> {3, new Vertex(1), "hello"});

            var equals = firstPath.Equals(secondPath);

            Assert.False(equals);
        }

        [Fact]
        public void ShouldReturnTrueForEqualsObjectOfTwoEqualPaths()
        {
            var firstPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> { 1, new Vertex(1), "hello" });
            object secondPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> { 1, new Vertex(1), "hello" });

            var equals = firstPath.Equals(secondPath);

            Assert.True(equals);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsObjectOfPathsWithDifferentLabels()
        {
            var firstPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> { 1, new Vertex(1), "hello" });
            object secondPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> { 1, new Vertex(1), "hello" });

            var equals = firstPath.Equals(secondPath);

            Assert.False(equals);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsObjectOfPathsWithDifferentObjects()
        {
            var firstPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> { 1, new Vertex(1), "hello" });
            object secondPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> { 3, new Vertex(1), "hello" });

            var equals = firstPath.Equals(secondPath);

            Assert.False(equals);
        }

        [Fact]
        public void ShouldReturnFalseForEqualsWhereOtherIsNull()
        {
            var path = new Path(new List<ISet<string>> {new HashSet<string> {"a", "b"},}, new List<object> {1});

            var equals = path.Equals(null);

            Assert.False(equals);
        }

        [Fact]
        public void ShouldReturnEqualHashcodesForEqualPaths()
        {
            var firstPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> { 1, new Vertex(1), "hello" });
            var secondPath =
                new Path(
                    new List<ISet<string>>
                    {
                        new HashSet<string> {"a", "b"},
                        new HashSet<string> {"c", "b"},
                        new HashSet<string>()
                    }, new List<object> { 1, new Vertex(1), "hello" });

            var firstHashCode = firstPath.GetHashCode();
            var secondHashCode = secondPath.GetHashCode();

            Assert.Equal(firstHashCode, secondHashCode);
        }

        [Fact]
        public void ShouldThrowWhenInvalidIndexIsAccessed()
        {
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(new List<ISet<string>>(), objects);

            Assert.Throws<ArgumentOutOfRangeException>(() => path[3]);
        }

        [Fact]
        public void ShouldReturnObjectsByTheirIndex()
        {
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(new List<ISet<string>>(), objects);

            Assert.Equal(1, path[0]);
            Assert.Equal(new Vertex(1), path[1]);
            Assert.Equal("hello", path[2]);
        }

        [Fact]
        public void ShouldReturnAllObjectsWhenTheirKeyIsAccessed()
        {
            var labels = new List<ISet<string>>
            {
                new HashSet<string> {"a", "b"},
                new HashSet<string> {"c", "b"},
                new HashSet<string>()
            };
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(labels, objects);

            var bObjects = path["b"];

            Assert.Equal(new List<object> {1, new Vertex(1)}, bObjects);
        }

        [Fact]
        public void ShouldReturnObjectsByTheirKey()
        {
            var labels = new List<ISet<string>>
            {
                new HashSet<string> {"a"},
                new HashSet<string> {"c", "b"},
                new HashSet<string>()
            };
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(labels, objects);

            Assert.Equal(1, path["a"]);
            Assert.Equal(new Vertex(1), path["c"]);
            Assert.Equal(new Vertex(1), path["b"]);
        }

        [Fact]
        public void ShouldThrowWhenUnknownKeyIsAccessed()
        {
            var path = new Path(new List<ISet<string>>(), new List<object>());

            Assert.Throws<KeyNotFoundException>(() => path["unknownKey"]);
        }

        [Fact]
        public void ShouldReturnCommonStringRepresentationForToString()
        {
            var labels = new List<ISet<string>>
            {
                new HashSet<string> {"a", "b"},
                new HashSet<string> {"c", "b"},
                new HashSet<string>()
            };
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(labels, objects);

            var pathStr = path.ToString();

            Assert.Equal("[1, v[1], hello]", pathStr);
        }

        [Fact]
        public void ShouldReturnTrueAndObjectsForTryGetWhenKeyWithMultipleObjectsIsProvided()
        {
            var labels = new List<ISet<string>>
            {
                new HashSet<string> {"a", "b"},
                new HashSet<string> {"c", "b"},
                new HashSet<string>()
            };
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(labels, objects);

            var success = path.TryGetValue("b", out object actualObj);

            Assert.True(success);
            Assert.Equal(new List<object> {1, new Vertex(1)}, actualObj);
        }

        [Fact]
        public void ShouldReturnTrueAndCorrectObjectForTryGet()
        {
            var labels = new List<ISet<string>>
            {
                new HashSet<string> {"a"},
                new HashSet<string> {"c", "b"},
                new HashSet<string>()
            };
            var objects = new List<object> {1, new Vertex(1), "hello"};
            var path = new Path(labels, objects);

            var success = path.TryGetValue("b", out object actualObj);

            Assert.True(success);
            Assert.Equal(new Vertex(1), actualObj);
        }

        [Fact]
        public void ShouldReturnFalseForTryGetWhenUnknownKeyIsProvided()
        {
            var path = new Path(new List<ISet<string>>(), new List<object>());

            var success = path.TryGetValue("unknownKey", out object _);

            Assert.False(success);
        }
    }
}