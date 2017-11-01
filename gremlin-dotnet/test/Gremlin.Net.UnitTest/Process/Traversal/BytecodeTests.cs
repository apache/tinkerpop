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

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Gremlin.Net.Process.Traversal;
using Xunit;

namespace Gremlin.Net.UnitTest.Process.Traversal
{
    public class BytecodeTests
    {
        [Fact]
        public void ShouldUseBingingsForSimpleValueInStepArgument()
        {
            var bytecode = new Bytecode();
            var bindings = Bindings.Instance;

            bytecode.AddStep("hasLabel", bindings.Of("label", "testvalue"));

            Assert.Equal(new Binding("label", "testvalue"), bytecode.StepInstructions[0].Arguments[0]);
        }

        [Fact]
        public void ShouldUseBindingsInsideArrayInStepArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddStep("someStep", "test", new[] {b.Of("arrayVariable", "arrayValue")});

            Assert.Equal(new Binding("arrayVariable", "arrayValue"), bytecode.StepInstructions[0].Arguments[1]);
        }

        [Fact]
        public void ShouldUseBindingsInsideDictionaryValuesInStepArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddStep("someStep", new Dictionary<string, object> {{"someKey", b.Of("valVariable", "valValue")}});

            var arg = bytecode.StepInstructions[0].Arguments[0] as IDictionary;
            Assert.Equal(new Binding("valVariable", "valValue"), arg["someKey"]);
        }

        [Fact]
        public void ShouldUseBindingsInsideDictionaryKeysInStepArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddStep("someStep", new Dictionary<string, object> {{b.Of("keyVariable", "keyValue"), 1234}});

            var arg = bytecode.StepInstructions[0].Arguments[0];
            var binding = ((Dictionary<object, object>) arg).Keys.First() as Binding;
            Assert.Equal(new Binding("keyVariable", "keyValue"), binding);
        }

        [Fact]
        public void ShouldUseBindingsInsideListInStepArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddStep("someStep", new List<string> {"test", b.Of("listVariable", "listValue")});

            var arg = bytecode.StepInstructions[0].Arguments[0] as IList;
            Assert.Equal(new Binding("listVariable", "listValue"), arg[1]);
        }

        [Fact]
        public void ShouldUseBindingsInsideHashSetInStepArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddStep("someStep", new HashSet<string> { "test", b.Of("setVariable", "setValue") });

            var arg = bytecode.StepInstructions[0].Arguments[0] as ISet<object>;
            Assert.Equal(new Binding("setVariable", "setValue"), arg.ToList()[1]);
        }

        [Fact]
        public void ShouldUseBingingsForSimpleValueInSourceArgument()
        {
            var bytecode = new Bytecode();
            var bindings = Bindings.Instance;

            bytecode.AddSource("hasLabel", bindings.Of("label", "testvalue"));

            Assert.Equal(new Binding("label", "testvalue"), bytecode.SourceInstructions[0].Arguments[0]);
        }

        [Fact]
        public void ShouldUseBindingsInsideArrayInSourceArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddSource("someSource", "test", new[] { b.Of("arrayVariable", "arrayValue") });

            Assert.Equal(new Binding("arrayVariable", "arrayValue"), bytecode.SourceInstructions[0].Arguments[1]);
        }

        [Fact]
        public void ShouldUseBindingsInsideDictionaryValuesInSourceArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddSource("someSource", new Dictionary<string, object> { { "someKey", b.Of("valVariable", "valValue") } });

            var arg = bytecode.SourceInstructions[0].Arguments[0] as IDictionary;
            Assert.Equal(new Binding("valVariable", "valValue"), arg["someKey"]);
        }

        [Fact]
        public void ShouldUseBindingsInsideDictionaryKeysInSourceArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddSource("someSource", new Dictionary<string, object> { { b.Of("keyVariable", "keyValue"), 1234 } });

            var arg = bytecode.SourceInstructions[0].Arguments[0];
            var binding = ((Dictionary<object, object>)arg).Keys.First() as Binding;
            Assert.Equal(new Binding("keyVariable", "keyValue"), binding);
        }

        [Fact]
        public void ShouldUseBindingsInsideListInSourceArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddSource("someSource", new List<string> { "test", b.Of("listVariable", "listValue") });

            var arg = bytecode.SourceInstructions[0].Arguments[0] as IList;
            Assert.Equal(new Binding("listVariable", "listValue"), arg[1]);
        }

        [Fact]
        public void ShouldUseBindingsInsideHashSetInSourceArgument()
        {
            var bytecode = new Bytecode();
            var b = Bindings.Instance;

            bytecode.AddSource("someSource", new HashSet<string> { "test", b.Of("setVariable", "setValue") });

            var arg = bytecode.SourceInstructions[0].Arguments[0] as ISet<object>;
            Assert.Equal(new Binding("setVariable", "setValue"), arg.ToList()[1]);
        }
    }
}