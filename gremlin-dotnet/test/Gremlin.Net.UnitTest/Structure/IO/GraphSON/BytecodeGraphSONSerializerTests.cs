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
using Gremlin.Net.Structure.IO.GraphSON;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure.IO.GraphSON
{
    public class BytecodeGraphSONSerializerTests
    {
        private GraphSONWriter CreateGraphSONWriter()
        {
            return new GraphSONWriter();
        }

        [Fact]
        public void ShouldSerializeByteCodeWithNestedTraversal()
        {
            var bytecode = new Bytecode();
            bytecode.AddStep("V");
            var nestedBytecode = new Bytecode();
            var nestedTraversal = new TestTraversal(nestedBytecode);
            nestedBytecode.AddStep("out");
            bytecode.AddStep("repeat", nestedTraversal);
            var graphsonWriter = CreateGraphSONWriter();

            var graphSON = graphsonWriter.WriteObject(bytecode);

            var expectedGraphSon =
                "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\"],[\"repeat\",{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"out\"]]}}]]}}";
            Assert.Equal(expectedGraphSon, graphSON);
        }

        [Fact]
        public void ShouldSerializeBytecodeWithNumbers()
        {
            var bytecode = new Bytecode();
            bytecode.AddStep("V", (long) 1);
            bytecode.AddStep("has", "age", 20);
            bytecode.AddStep("has", "height", 6.5);
            var graphsonWriter = CreateGraphSONWriter();

            var graphSON = graphsonWriter.WriteObject(bytecode);

            var expectedGraphSon =
                "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\",{\"@type\":\"g:Int64\",\"@value\":1}],[\"has\",\"age\",{\"@type\":\"g:Int32\",\"@value\":20}],[\"has\",\"height\",{\"@type\":\"g:Double\",\"@value\":6.5}]]}}";
            Assert.Equal(expectedGraphSon, graphSON);
        }

        [Fact]
        public void ShouldSerialize_g_V()
        {
            var bytecode = new Bytecode();
            bytecode.AddStep("V");
            var graphsonWriter = CreateGraphSONWriter();

            var graphSON = graphsonWriter.WriteObject(bytecode);

            Assert.Equal("{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\"]]}}", graphSON);
        }

        [Fact]
        public void ShouldSerialize_g_V_Count()
        {
            var bytecode = new Bytecode();
            bytecode.AddStep("V");
            bytecode.AddStep("count");
            var graphsonWriter = CreateGraphSONWriter();

            var graphSON = graphsonWriter.WriteObject(bytecode);

            var expectedGraphSon = "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\"],[\"count\"]]}}";
            Assert.Equal(expectedGraphSon, graphSON);
        }

        [Fact]
        public void ShouldSerialize_g_V_HasXPerson_Name_GremlinX_Count()
        {
            var bytecode = new Bytecode();
            bytecode.AddStep("V");
            bytecode.AddStep("has", "Person", "Name", "Gremlin");
            bytecode.AddStep("count");
            var graphsonWriter = CreateGraphSONWriter();

            var graphSON = graphsonWriter.WriteObject(bytecode);

            var expectedGraphSon =
                "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\"],[\"has\",\"Person\",\"Name\",\"Gremlin\"],[\"count\"]]}}";
            Assert.Equal(expectedGraphSon, graphSON);
        }

        [Fact]
        public void ShouldSerializeBytecodeWithSourcesStep()
        {
            var bytecode = new Bytecode();
            bytecode.AddSource("withSideEffect", "a", new List<string> {"josh", "peter"});
            bytecode.AddStep("V", 1);
            bytecode.AddStep("values", "name");
            bytecode.AddStep("where", new TraversalPredicate("within", "a"));
            var graphsonWriter = CreateGraphSONWriter();

            var graphSON = graphsonWriter.WriteObject(bytecode);

            var expectedGraphSon =
                "{\"@type\":\"g:Bytecode\",\"@value\":{\"source\":[[\"withSideEffect\",\"a\",[\"josh\",\"peter\"]]],\"step\":[[\"V\",{\"@type\":\"g:Int32\",\"@value\":1}],[\"values\",\"name\"],[\"where\",{\"@type\":\"g:P\",\"@value\":{\"predicate\":\"within\",\"value\":\"a\"}}]]}}";
            Assert.Equal(expectedGraphSon, graphSON);
        }

        [Fact]
        public void ShouldSerializeBytecodeWithBindings()
        {
            var bytecode = new Bytecode();
            bytecode.AddStep("V", new Binding("id", 123));
            var graphsonWriter = CreateGraphSONWriter();

            var graphSon = graphsonWriter.WriteObject(bytecode);

            var expectedGraphSon =
                "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"V\",{\"@type\":\"g:Binding\",\"@value\":{\"value\":{\"@type\":\"g:Int32\",\"@value\":123},\"key\":\"id\"}}]]}}";
            Assert.Equal(expectedGraphSon, graphSon);
        }
    }

    internal class TestTraversal : DefaultTraversal<object, object>
    {
        public TestTraversal(Bytecode bytecode)
        {
            Bytecode = bytecode;
        }
    }
}