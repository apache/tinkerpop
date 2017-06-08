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

using Gremlin.CSharp.Process;
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;

namespace Gremlin.CSharp.IntegrationTest
{
    public class GraphSONWriterTests
    {
        [Fact]
        public void ShouldSerializeLongPredicateCorrectly()
        {
            var writer = CreateStandardGraphSONWriter();
            var predicate = P.Lt("b").Or(P.Gt("c")).And(P.Neq("d"));

            var graphSon = writer.WriteObject(predicate);

            const string expected =
                "{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"and\",\"value\":[{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"or\",\"value\":[{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"lt\",\"value\":\"b\"}},{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"gt\",\"value\":\"c\"}}]}},{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"neq\",\"value\":\"d\"}}]}}";
            Assert.Equal(expected, graphSon);
        }

        private GraphSONWriter CreateStandardGraphSONWriter()
        {
            return new GraphSONWriter();
        }
    }
}