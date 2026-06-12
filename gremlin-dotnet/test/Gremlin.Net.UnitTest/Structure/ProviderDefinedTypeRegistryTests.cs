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

using System;
using System.Collections.Generic;
using System.Reflection;
using Gremlin.Net.Structure;
using Xunit;

namespace Gremlin.Net.UnitTest.Structure
{
    public class ProviderDefinedTypeRegistryTests
    {
        [Fact]
        public void ShouldHydrateToTypedObjectWhenAdapterRegistered()
        {
            var registry = new ProviderDefinedTypeRegistry();
            registry.Register(new PointAdapter());
            var pdt = new ProviderDefinedType("geo:Point",
                new Dictionary<string, object?> { ["x"] = 1.0, ["y"] = 2.0 });

            var result = registry.Hydrate(pdt);

            var point = Assert.IsType<Point>(result);
            Assert.Equal(1.0, point.X);
            Assert.Equal(2.0, point.Y);
        }

        [Fact]
        public void ShouldReturnRawPdtWhenNoAdapterRegistered()
        {
            var registry = new ProviderDefinedTypeRegistry();
            var pdt = new ProviderDefinedType("unknown:Type",
                new Dictionary<string, object?> { ["a"] = "b" });

            var result = registry.Hydrate(pdt);

            Assert.Same(pdt, result);
        }

        [Fact]
        public void ShouldReturnRawPdtWhenAdapterThrows()
        {
            var registry = new ProviderDefinedTypeRegistry();
            registry.Register(new ThrowingAdapter());
            var pdt = new ProviderDefinedType("bad:Type",
                new Dictionary<string, object?> { ["x"] = "oops" });

            var result = registry.Hydrate(pdt);

            Assert.Same(pdt, result);
        }

        [Fact]
        public void ShouldHydrateNestedPdt()
        {
            var registry = new ProviderDefinedTypeRegistry();
            registry.Register(new PointAdapter());
            registry.Register(new LineAdapter());
            var startPdt = new ProviderDefinedType("geo:Point",
                new Dictionary<string, object?> { ["x"] = 0.0, ["y"] = 0.0 });
            var endPdt = new ProviderDefinedType("geo:Point",
                new Dictionary<string, object?> { ["x"] = 3.0, ["y"] = 4.0 });
            var linePdt = new ProviderDefinedType("geo:Line",
                new Dictionary<string, object?> { ["start"] = startPdt, ["end"] = endPdt });

            var result = registry.Hydrate(linePdt);

            var line = Assert.IsType<Line>(result);
            Assert.Equal(0.0, line.Start.X);
            Assert.Equal(0.0, line.Start.Y);
            Assert.Equal(3.0, line.End.X);
            Assert.Equal(4.0, line.End.Y);
        }

        [Fact]
        public void ShouldHaveProviderDefinedAttributeWithNameProperty()
        {
            var attr = typeof(AnnotatedPoint).GetCustomAttribute<ProviderDefinedAttribute>();

            Assert.NotNull(attr);
            Assert.Equal("geo:Point", attr!.Name);
        }

        [Fact]
        public void CreateShouldReturnRegistryWithoutCrashing()
        {
            var registry = ProviderDefinedTypeRegistry.Create();

            Assert.NotNull(registry);
        }

        [Fact]
        public void CreateShouldDiscoverAdapterFromAssembly()
        {
            var registry = ProviderDefinedTypeRegistry.Create();
            var pdt = new ProviderDefinedType("test:Discoverable",
                new Dictionary<string, object?> { ["value"] = "hello" });

            var result = registry.Hydrate(pdt);

            var obj = Assert.IsType<DiscoverableType>(result);
            Assert.Equal("hello", obj.Value);
        }

        [Fact]
        public void ShouldHydrateRegisteredInnerInsideUnregisteredOuter()
        {
            // Contract: a registered/adapted inner PDT always hydrates even when nested inside an
            // unregistered outer PDT. Register only the inner type adapter, NOT the outer.
            var registry = new ProviderDefinedTypeRegistry();
            registry.Register(new InnerPointAdapter());

            var innerPdt = new ProviderDefinedType("nested:Point",
                new Dictionary<string, object?> { ["x"] = 3.0, ["y"] = 4.0 });
            var outerPdt = new ProviderDefinedType("unregistered:Container",
                new Dictionary<string, object?> { ["location"] = innerPdt, ["label"] = "test" });

            var result = registry.Hydrate(outerPdt);

            // Outer stays raw PDT since it has no adapter
            var rawOuter = Assert.IsType<ProviderDefinedType>(result);
            Assert.Equal("unregistered:Container", rawOuter.Name);

            // The inner field SHOULD be hydrated to InnerPoint because it IS registered
            var locationValue = rawOuter.Fields["location"];
            var hydratedInner = Assert.IsType<InnerPoint>(locationValue);
            Assert.Equal(3.0, hydratedInner.X);
            Assert.Equal(4.0, hydratedInner.Y);

            // Non-PDT fields should be unchanged
            Assert.Equal("test", rawOuter.Fields["label"]);
        }

        #region Test helpers

        private class Point
        {
            public double X { get; init; }
            public double Y { get; init; }
        }

        private class Line
        {
            public Point Start { get; init; } = null!;
            public Point End { get; init; } = null!;
        }

        [ProviderDefined(Name = "geo:Point")]
        private class AnnotatedPoint { }

        private class PointAdapter : IProviderDefinedTypeAdapter<Point>
        {
            public string TypeName => "geo:Point";

            public Point FromFields(IReadOnlyDictionary<string, object?> fields) =>
                new() { X = (double)fields["x"]!, Y = (double)fields["y"]! };

            public IReadOnlyDictionary<string, object?> ToFields(Point obj) =>
                new Dictionary<string, object?> { ["x"] = obj.X, ["y"] = obj.Y };
        }

        private class LineAdapter : IProviderDefinedTypeAdapter<Line>
        {
            public string TypeName => "geo:Line";

            public Line FromFields(IReadOnlyDictionary<string, object?> fields) =>
                new() { Start = (Point)fields["start"]!, End = (Point)fields["end"]! };

            public IReadOnlyDictionary<string, object?> ToFields(Line obj) =>
                new Dictionary<string, object?> { ["start"] = obj.Start, ["end"] = obj.End };
        }

        private class ThrowingAdapter : IProviderDefinedTypeAdapter<object>
        {
            public string TypeName => "bad:Type";

            public object FromFields(IReadOnlyDictionary<string, object?> fields) =>
                throw new InvalidOperationException("intentional failure");

            public IReadOnlyDictionary<string, object?> ToFields(object obj) =>
                throw new InvalidOperationException("intentional failure");
        }

        private class InnerPoint
        {
            public double X { get; init; }
            public double Y { get; init; }
        }

        private class InnerPointAdapter : IProviderDefinedTypeAdapter<InnerPoint>
        {
            public string TypeName => "nested:Point";

            public InnerPoint FromFields(IReadOnlyDictionary<string, object?> fields) =>
                new() { X = (double)fields["x"]!, Y = (double)fields["y"]! };

            public IReadOnlyDictionary<string, object?> ToFields(InnerPoint obj) =>
                new Dictionary<string, object?> { ["x"] = obj.X, ["y"] = obj.Y };
        }

        #endregion
    }

    /// <summary>Test type discoverable by Build() assembly scanning.</summary>
    public class DiscoverableType
    {
        public string Value { get; init; } = "";
    }

    /// <summary>Test adapter discoverable by Build() assembly scanning.</summary>
    public class DiscoverableTypeAdapter : IProviderDefinedTypeAdapter<DiscoverableType>
    {
        public string TypeName => "test:Discoverable";

        public DiscoverableType FromFields(IReadOnlyDictionary<string, object?> fields) =>
            new() { Value = (string)fields["value"]! };

        public IReadOnlyDictionary<string, object?> ToFields(DiscoverableType obj) =>
            new Dictionary<string, object?> { ["value"] = obj.Value };
    }
}
