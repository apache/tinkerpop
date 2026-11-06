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
using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphBinary4;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure
{
    public class ProviderDefinedAttributeTests
    {
        [Fact]
        public void ShouldHydrateNestedAnnotatedPdt()
        {
            ProviderDefinedAttribute.RegisteredTypes["test:Address"] = typeof(TestAddress);
            ProviderDefinedAttribute.RegisteredTypes["test:Person"] = typeof(TestPerson);
            try
            {
                var addressPdt = new CompositePDT("test:Address",
                    new Dictionary<string, object?> { ["Street"] = "123 Main", ["City"] = "Springfield" });
                var personPdt = new CompositePDT("test:Person",
                    new Dictionary<string, object?> { ["Name"] = "Alice", ["Address"] = addressPdt });

                var result = ProviderDefinedAttribute.HydrateIfRegistered(personPdt);

                var person = Assert.IsType<TestPerson>(result);
                Assert.Equal("Alice", person.Name);
                Assert.NotNull(person.Address);
                Assert.Equal("123 Main", person.Address!.Street);
                Assert.Equal("Springfield", person.Address.City);
            }
            finally
            {
                ProviderDefinedAttribute.RegisteredTypes.TryRemove("test:Address", out _);
                ProviderDefinedAttribute.RegisteredTypes.TryRemove("test:Person", out _);
            }
        }

        [Fact]
        public void ShouldReturnRawPdtWhenFieldConversionFails()
        {
            ProviderDefinedAttribute.RegisteredTypes["test:Bad"] = typeof(TestBadTarget);
            try
            {
                var pdt = new CompositePDT("test:Bad",
                    new Dictionary<string, object?> { ["Count"] = "not-an-int" });

                var result = ProviderDefinedAttribute.HydrateIfRegistered(pdt);

                Assert.Same(pdt, result);
            }
            finally
            {
                ProviderDefinedAttribute.RegisteredTypes.TryRemove("test:Bad", out _);
            }
        }

        [Fact]
        public void ShouldReturnRawPdtWhenNestedTypeNotRegistered()
        {
            ProviderDefinedAttribute.RegisteredTypes["test:Outer"] = typeof(TestOuter);
            try
            {
                var nestedPdt = new CompositePDT("test:Unknown",
                    new Dictionary<string, object?> { ["X"] = 1 });
                var outerPdt = new CompositePDT("test:Outer",
                    new Dictionary<string, object?> { ["Inner"] = nestedPdt });

                // Inner is typed as TestInner but nested PDT can't hydrate — Convert.ChangeType
                // would throw, but the try/catch returns raw pdt gracefully.
                var result = ProviderDefinedAttribute.HydrateIfRegistered(outerPdt);

                Assert.Same(outerPdt, result);
            }
            finally
            {
                ProviderDefinedAttribute.RegisteredTypes.TryRemove("test:Outer", out _);
            }
        }

        [Fact]
        public async Task ShouldNotThrowFromReaderWhenHydrationFails()
        {
            ProviderDefinedAttribute.RegisteredTypes["test:Bad"] = typeof(TestBadTarget);
            try
            {
                var pdt = new CompositePDT("test:Bad",
                    new Dictionary<string, object?> { ["Count"] = "not-an-int" });

                var writer = new GraphBinaryWriter();
                var reader = new GraphBinaryReader();

                using var stream = new MemoryStream();
                await writer.WriteAsync(pdt, stream);
                stream.Position = 0;
                var result = await reader.ReadAsync(stream);

                // Should return raw PDT, not throw
                Assert.IsType<CompositePDT>(result);
            }
            finally
            {
                ProviderDefinedAttribute.RegisteredTypes.TryRemove("test:Bad", out _);
            }
        }

        [Fact]
        public void ShouldReturnRawPdtWhenTypeNotRegistered()
        {
            var pdt = new CompositePDT("test:Unregistered",
                new Dictionary<string, object?> { ["X"] = 1 });

            var result = ProviderDefinedAttribute.HydrateIfRegistered(pdt);

            Assert.Same(pdt, result);
        }

        [Fact]
        public void ShouldHydrateRegisteredInnerInsideUnregisteredOuter()
        {
            // Register ONLY the inner type, NOT the outer
            ProviderDefinedAttribute.RegisteredTypes["test:InnerWidget"] = typeof(TestInnerWidget);
            try
            {
                var innerPdt = new CompositePDT("test:InnerWidget",
                    new Dictionary<string, object?> { ["Label"] = "hello" });
                var outerPdt = new CompositePDT("test:UnregisteredOuter",
                    new Dictionary<string, object?> { ["Widget"] = innerPdt, ["Count"] = 5 });

                // Act
                var result = ProviderDefinedAttribute.HydrateIfRegistered(outerPdt);

                // Assert: outer is unregistered so stays raw PDT
                var rawOuter = Assert.IsType<CompositePDT>(result);
                Assert.Equal("test:UnregisteredOuter", rawOuter.Name);

                // The inner field SHOULD be hydrated because it IS registered
                var widgetValue = rawOuter.Fields["Widget"];
                var hydratedInner = Assert.IsType<TestInnerWidget>(widgetValue);
                Assert.Equal("hello", hydratedInner.Label);

                // Non-PDT fields unchanged
                Assert.Equal(5, rawOuter.Fields["Count"]);
            }
            finally
            {
                ProviderDefinedAttribute.RegisteredTypes.TryRemove("test:InnerWidget", out _);
            }
        }

        #region Test helpers

        [ProviderDefined(Name = "test:Address")]
        public class TestAddress
        {
            public string Street { get; set; } = "";
            public string City { get; set; } = "";
        }

        [ProviderDefined(Name = "test:Person")]
        public class TestPerson
        {
            public string Name { get; set; } = "";
            public TestAddress? Address { get; set; }
        }

        [ProviderDefined(Name = "test:Bad")]
        public class TestBadTarget
        {
            public int Count { get; set; }
        }

        [ProviderDefined(Name = "test:Outer")]
        public class TestOuter
        {
            public TestInner? Inner { get; set; }
        }

        [ProviderDefined(Name = "test:Inner")]
        public class TestInner
        {
            public int X { get; set; }
        }

        [ProviderDefined(Name = "test:InnerWidget")]
        public class TestInnerWidget
        {
            public string Label { get; set; } = "";
        }

        #endregion
    }
}
