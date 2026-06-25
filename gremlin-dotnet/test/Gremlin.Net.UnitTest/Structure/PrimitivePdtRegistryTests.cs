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
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure
{
    public class PrimitivePdtRegistryTests
    {
        [Fact]
        public void ShouldHydratePrimitiveWhenAdapterRegistered()
        {
            var registry = new ProviderDefinedTypeRegistry();
            registry.RegisterPrimitive(new Uint32Adapter());
            var pdt = new PrimitiveProviderDefinedType("test:Uint32", "42");

            var result = registry.HydratePrimitive(pdt);

            Assert.IsType<uint>(result);
            Assert.Equal(42u, (uint)result);
        }

        [Fact]
        public void ShouldReturnRawPrimitivePdtWhenNoAdapterRegistered()
        {
            var registry = new ProviderDefinedTypeRegistry();
            var pdt = new PrimitiveProviderDefinedType("unknown:Type", "hello");

            var result = registry.HydratePrimitive(pdt);

            Assert.Same(pdt, result);
        }

        [Fact]
        public void ShouldReturnRawPrimitivePdtWhenAdapterThrows()
        {
            var registry = new ProviderDefinedTypeRegistry();
            registry.RegisterPrimitive(new ThrowingPrimitiveAdapter());
            var pdt = new PrimitiveProviderDefinedType("bad:Type", "oops");

            var result = registry.HydratePrimitive(pdt);

            Assert.Same(pdt, result);
        }

        [Fact]
        public void ShouldHydratePrimitiveNestedInComposite()
        {
            var registry = new ProviderDefinedTypeRegistry();
            registry.RegisterPrimitive(new Uint32Adapter());
            var inner = new PrimitiveProviderDefinedType("test:Uint32", "99");
            var outer = new ProviderDefinedType("unregistered:Wrapper",
                new Dictionary<string, object?> { ["val"] = inner, ["label"] = "test" });

            var result = registry.HydrateComposite(outer);

            var rawOuter = Assert.IsType<ProviderDefinedType>(result);
            Assert.Equal(99u, (uint)rawOuter.Fields["val"]!);
            Assert.Equal("test", rawOuter.Fields["label"]);
        }

        [Fact]
        public void ShouldGetPrimitiveAdapterByType()
        {
            var registry = new ProviderDefinedTypeRegistry();
            registry.RegisterPrimitive(new Uint32Adapter());

            var info = registry.GetPrimitiveAdapterByType(typeof(uint));

            Assert.NotNull(info);
            Assert.Equal("test:Uint32", info!.Value.typeName);
            Assert.Equal("123", info.Value.Item2(123u));
        }

        [Fact]
        public void ShouldReturnNullForUnregisteredPrimitiveType()
        {
            var registry = new ProviderDefinedTypeRegistry();

            var info = registry.GetPrimitiveAdapterByType(typeof(uint));

            Assert.Null(info);
        }

        #region Test helpers

        private class Uint32Adapter : IPrimitivePdtAdapter<uint>
        {
            public string TypeName => "test:Uint32";
            public uint FromString(string value) => uint.Parse(value);
            public string ToString(uint obj) => obj.ToString();
        }

        private class ThrowingPrimitiveAdapter : IPrimitivePdtAdapter<object>
        {
            public string TypeName => "bad:Type";
            public object FromString(string value) => throw new InvalidOperationException("intentional");
            public string ToString(object obj) => throw new InvalidOperationException("intentional");
        }

        #endregion
    }
}
