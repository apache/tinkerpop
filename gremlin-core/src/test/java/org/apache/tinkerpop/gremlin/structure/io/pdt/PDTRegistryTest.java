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
package org.apache.tinkerpop.gremlin.structure.io.pdt;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PDTRegistryTest {

    // Simple test type
    static class Point {
        final int x;
        final int y;
        Point(int x, int y) { this.x = x; this.y = y; }
    }

    static class PointAdapter implements CompositePDTAdapter<Point> {
        @Override public String typeName() { return "Point"; }
        @Override public Class<Point> targetClass() { return Point.class; }
        @Override public Map<String, Object> toFields(Point obj) {
            final Map<String, Object> m = new HashMap<>();
            m.put("x", obj.x);
            m.put("y", obj.y);
            return m;
        }
        @Override public Point fromFields(Map<String, Object> fields) {
            return new Point((int) fields.get("x"), (int) fields.get("y"));
        }
    }

    // Nested test type
    static class Line {
        final Point start;
        final Point end;
        Line(Point start, Point end) { this.start = start; this.end = end; }
    }

    static class LineAdapter implements CompositePDTAdapter<Line> {
        @Override public String typeName() { return "Line"; }
        @Override public Class<Line> targetClass() { return Line.class; }
        @Override public Map<String, Object> toFields(Line obj) {
            final Map<String, Object> m = new HashMap<>();
            m.put("start", obj.start);
            m.put("end", obj.end);
            return m;
        }
        @Override public Line fromFields(Map<String, Object> fields) {
            return new Line((Point) fields.get("start"), (Point) fields.get("end"));
        }
    }

    // Adapter that always throws
    static class FailingAdapter implements CompositePDTAdapter<Point> {
        @Override public String typeName() { return "Failing"; }
        @Override public Class<Point> targetClass() { return Point.class; }
        @Override public Map<String, Object> toFields(Point obj) { return new HashMap<>(); }
        @Override public Point fromFields(Map<String, Object> fields) {
            throw new RuntimeException("intentional failure");
        }
    }

    @Test
    public void shouldHydrateSimplePdt() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new PointAdapter());

        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 3);
        fields.put("y", 7);
        final CompositePDT pdt = new CompositePDT("Point", fields);

        final Object result = registry.hydrate(pdt);
        assertTrue(result instanceof Point);
        assertEquals(3, ((Point) result).x);
        assertEquals(7, ((Point) result).y);
    }

    @Test
    public void shouldReturnRawPdtWhenNoAdapterRegistered() {
        final PDTRegistry registry = PDTRegistry.empty();

        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 1);
        final CompositePDT pdt = new CompositePDT("Unknown", fields);

        final Object result = registry.hydrate(pdt);
        assertSame(pdt, result);
    }

    @Test
    public void shouldHydrateNestedPdts() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new PointAdapter());
        registry.register(new LineAdapter());

        final Map<String, Object> startFields = new HashMap<>();
        startFields.put("x", 0);
        startFields.put("y", 0);
        final Map<String, Object> endFields = new HashMap<>();
        endFields.put("x", 5);
        endFields.put("y", 5);

        final Map<String, Object> lineFields = new HashMap<>();
        lineFields.put("start", new CompositePDT("Point", startFields));
        lineFields.put("end", new CompositePDT("Point", endFields));
        final CompositePDT linePdt = new CompositePDT("Line", lineFields);

        final Object result = registry.hydrate(linePdt);
        assertTrue(result instanceof Line);
        final Line line = (Line) result;
        assertEquals(0, line.start.x);
        assertEquals(0, line.start.y);
        assertEquals(5, line.end.x);
        assertEquals(5, line.end.y);
    }

    @Test
    public void shouldPartiallyHydrateWhenInnerAdapterMissing() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new LineAdapter());
        // Point adapter NOT registered

        final Map<String, Object> startFields = new HashMap<>();
        startFields.put("x", 1);
        startFields.put("y", 2);
        final CompositePDT startPdt = new CompositePDT("Point", startFields);

        final Map<String, Object> endFields = new HashMap<>();
        endFields.put("x", 3);
        endFields.put("y", 4);
        final CompositePDT endPdt = new CompositePDT("Point", endFields);

        final Map<String, Object> lineFields = new HashMap<>();
        lineFields.put("start", startPdt);
        lineFields.put("end", endPdt);
        final CompositePDT linePdt = new CompositePDT("Line", lineFields);

        // Line adapter will receive CompositePDT values for start/end since Point is not registered.
        // The LineAdapter.fromFields casts to Point which will throw ClassCastException,
        // so hydrate should fall back to returning the raw PDT.
        final Object result = registry.hydrate(linePdt);
        assertSame(linePdt, result);
    }

    @Test
    public void shouldFallBackWhenAdapterThrows() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new FailingAdapter());

        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 1);
        final CompositePDT pdt = new CompositePDT("Failing", fields);

        // should not throw, should return raw PDT
        final Object result = registry.hydrate(pdt);
        assertSame(pdt, result);
    }

    @Test
    public void shouldLookUpAdapterByClass() {
        final PDTRegistry registry = PDTRegistry.empty();
        final PointAdapter adapter = new PointAdapter();
        registry.register(adapter);

        final Optional<CompositePDTAdapter<?>> found = registry.getCompositeAdapterByClass(Point.class);
        assertTrue(found.isPresent());
        assertEquals("Point", found.get().typeName());
    }

    // Collection test type
    static class Polygon {
        final List<Point> vertices;
        Polygon(List<Point> vertices) { this.vertices = vertices; }
    }

    static class PolygonAdapter implements CompositePDTAdapter<Polygon> {
        @Override public String typeName() { return "Polygon"; }
        @Override public Class<Polygon> targetClass() { return Polygon.class; }
        @Override public Map<String, Object> toFields(Polygon obj) {
            final Map<String, Object> m = new HashMap<>();
            m.put("vertices", obj.vertices);
            return m;
        }
        @SuppressWarnings("unchecked")
        @Override public Polygon fromFields(Map<String, Object> fields) {
            return new Polygon((List<Point>) fields.get("vertices"));
        }
    }

    @Test
    public void shouldHydratePdtsInsideList() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new PointAdapter());
        registry.register(new PolygonAdapter());

        final Map<String, Object> p1 = new HashMap<>();
        p1.put("x", 1); p1.put("y", 2);
        final Map<String, Object> p2 = new HashMap<>();
        p2.put("x", 3); p2.put("y", 4);

        final Map<String, Object> polyFields = new HashMap<>();
        polyFields.put("vertices", Arrays.asList(
                new CompositePDT("Point", p1),
                new CompositePDT("Point", p2)));
        final CompositePDT polyPdt = new CompositePDT("Polygon", polyFields);

        final Object result = registry.hydrate(polyPdt);
        assertTrue(result instanceof Polygon);
        final Polygon polygon = (Polygon) result;
        assertEquals(2, polygon.vertices.size());
        assertEquals(1, polygon.vertices.get(0).x);
        assertEquals(2, polygon.vertices.get(0).y);
        assertEquals(3, polygon.vertices.get(1).x);
        assertEquals(4, polygon.vertices.get(1).y);
    }

    @Test
    public void shouldHydratePdtsInsideMapValues() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new PointAdapter());

        // A simple adapter that receives a map of named points
        registry.register(new CompositePDTAdapter<Map>() {
            @Override public String typeName() { return "PointMap"; }
            @Override public Class<Map> targetClass() { return Map.class; }
            @Override public Map<String, Object> toFields(Map obj) { return new HashMap<>(); }
            @SuppressWarnings("unchecked")
            @Override public Map fromFields(Map<String, Object> fields) {
                return (Map<String, Object>) fields.get("points");
            }
        });

        final Map<String, Object> p1 = new HashMap<>();
        p1.put("x", 10); p1.put("y", 20);
        final Map<String, Object> p2 = new HashMap<>();
        p2.put("x", 30); p2.put("y", 40);

        final Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("origin", new CompositePDT("Point", p1));
        innerMap.put("target", new CompositePDT("Point", p2));

        final Map<String, Object> fields = new HashMap<>();
        fields.put("points", innerMap);
        final CompositePDT pdt = new CompositePDT("PointMap", fields);

        final Object result = registry.hydrate(pdt);
        assertTrue(result instanceof Map);
        @SuppressWarnings("unchecked")
        final Map<String, Object> resultMap = (Map<String, Object>) result;
        assertTrue(resultMap.get("origin") instanceof Point);
        assertTrue(resultMap.get("target") instanceof Point);
        assertEquals(10, ((Point) resultMap.get("origin")).x);
        assertEquals(40, ((Point) resultMap.get("target")).y);
    }

    @Test
    public void shouldBuildViaServiceLoader() {
        // ServiceLoader.load will find adapters on the classpath. With no META-INF/services file
        // in test scope, this should produce an empty registry that still functions.
        final PDTRegistry registry = PDTRegistry.create();

        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 1);
        final CompositePDT pdt = new CompositePDT("Unregistered", fields);
        final Object result = registry.hydrate(pdt);
        assertSame(pdt, result);
    }

    // Annotated test types for register(Class<?>...)
    @ProviderDefined(name = "AnnotatedPoint")
    static class AnnotatedPoint {
        public int x;
        public int y;
        public AnnotatedPoint() {}
        public AnnotatedPoint(int x, int y) { this.x = x; this.y = y; }
    }

    @ProviderDefined(name = "Excluded", excludedFields = {"secret"})
    static class ExcludedFields {
        public int value;
        public String secret;
        public ExcludedFields() {}
    }

    @ProviderDefined(name = "NoCtor")
    static class NoNoArgCtor {
        public int x;
        public NoNoArgCtor(int x) { this.x = x; }
    }

    static class NotAnnotated {
        public int x;
    }

    @Test
    public void shouldRegisterAndHydrateAnnotatedClass() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(AnnotatedPoint.class);

        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 3);
        fields.put("y", 7);
        final Object result = registry.hydrate(new CompositePDT("AnnotatedPoint", fields));

        assertTrue(result instanceof AnnotatedPoint);
        assertEquals(3, ((AnnotatedPoint) result).x);
        assertEquals(7, ((AnnotatedPoint) result).y);
    }

    @Test
    public void shouldDehydrateAnnotatedClassViaAdapter() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(AnnotatedPoint.class);

        final Optional<CompositePDTAdapter<?>> adapter = registry.getCompositeAdapterByClass(AnnotatedPoint.class);
        assertTrue(adapter.isPresent());
        assertEquals("AnnotatedPoint", adapter.get().typeName());
    }

    @Test
    public void shouldRespectExcludedFieldsWhenHydratingAnnotatedClass() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(ExcludedFields.class);

        final Map<String, Object> fields = new HashMap<>();
        fields.put("value", 42);
        fields.put("secret", "should-be-ignored");
        final Object result = registry.hydrate(new CompositePDT("Excluded", fields));

        assertTrue(result instanceof ExcludedFields);
        assertEquals(42, ((ExcludedFields) result).value);
        // secret is excluded from the field mapping, so it is not set
        assertEquals(null, ((ExcludedFields) result).secret);
    }

    @Test
    public void shouldThrowWhenRegisteringNonAnnotatedClass() {
        final PDTRegistry registry = PDTRegistry.empty();
        try {
            registry.register(NotAnnotated.class);
            fail("Expected IllegalArgumentException for non-annotated class");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("not annotated with @ProviderDefined"));
        }
    }

    @Test
    public void shouldThrowWhenRegisteringClassWithoutNoArgConstructor() {
        final PDTRegistry registry = PDTRegistry.empty();
        try {
            registry.register(NoNoArgCtor.class);
            fail("Expected IllegalArgumentException for class without no-arg constructor");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("no-arg constructor"));
        }
    }

    @Test
    public void shouldHydrateNestedRegisteredTypeInsideUnregisteredOuter() {
        // Contract: a registered type ALWAYS hydrates even when nested inside an unregistered outer PDT.
        // The outer "Container" has no adapter, so it remains a raw CompositePDT,
        // but the inner "Point" field value should be hydrated to a Point instance.
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new PointAdapter());

        final Map<String, Object> pointFields = new HashMap<>();
        pointFields.put("x", 10);
        pointFields.put("y", 20);
        final CompositePDT innerPointPdt = new CompositePDT("Point", pointFields);

        final Map<String, Object> containerFields = new HashMap<>();
        containerFields.put("location", innerPointPdt);
        final CompositePDT outerPdt = new CompositePDT("Container", containerFields);

        final Object result = registry.hydrate(outerPdt);

        // Outer should remain a raw CompositePDT (no adapter for "Container")
        assertTrue("Expected outer to remain a CompositePDT", result instanceof CompositePDT);
        final CompositePDT resultPdt = (CompositePDT) result;
        assertEquals("Container", resultPdt.getName());

        // Inner "location" field should be hydrated to a Point instance
        final Object innerValue = resultPdt.getFields().get("location");
        assertTrue("Expected inner field to be hydrated to Point but was: " +
                (innerValue == null ? "null" : innerValue.getClass().getName()),
                innerValue instanceof Point);
        assertEquals(10, ((Point) innerValue).x);
        assertEquals(20, ((Point) innerValue).y);
    }

    // === Primitive PDT Adapter tests ===

    static class Uint32 {
        final long value;
        Uint32(long value) { this.value = value; }
    }

    static class Uint32Adapter implements PrimitivePDTAdapter<Uint32> {
        @Override public String typeName() { return "Uint32"; }
        @Override public Class<Uint32> targetClass() { return Uint32.class; }
        @Override public String toValue(Uint32 obj) { return Long.toString(obj.value); }
        @Override public Uint32 fromValue(String value) { return new Uint32(Long.parseLong(value)); }
    }

    static class FailingPrimitiveAdapter implements PrimitivePDTAdapter<Uint32> {
        @Override public String typeName() { return "FailPrim"; }
        @Override public Class<Uint32> targetClass() { return Uint32.class; }
        @Override public String toValue(Uint32 obj) { return "0"; }
        @Override public Uint32 fromValue(String value) { throw new RuntimeException("intentional primitive failure"); }
    }

    @Test
    public void shouldRegisterAndLookUpPrimitiveAdapterByName() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new Uint32Adapter());

        final Optional<PrimitivePDTAdapter<?>> found = registry.getPrimitiveAdapterByName("Uint32");
        assertTrue(found.isPresent());
        assertEquals("Uint32", found.get().typeName());
    }

    @Test
    public void shouldRegisterAndLookUpPrimitiveAdapterByClass() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new Uint32Adapter());

        final Optional<PrimitivePDTAdapter<?>> found = registry.getPrimitiveAdapterByClass(Uint32.class);
        assertTrue(found.isPresent());
        assertEquals(Uint32.class, found.get().targetClass());
    }

    @Test
    public void shouldHydratePrimitive() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new Uint32Adapter());

        final PrimitivePDT pdt = new PrimitivePDT("Uint32", "42");
        final Object result = registry.hydratePrimitive(pdt);
        assertTrue(result instanceof Uint32);
        assertEquals(42L, ((Uint32) result).value);
    }

    @Test
    public void shouldReturnRawPrimitivePdtWhenNoAdapterRegistered() {
        final PDTRegistry registry = PDTRegistry.empty();

        final PrimitivePDT pdt = new PrimitivePDT("Unknown", "x");
        final Object result = registry.hydratePrimitive(pdt);
        assertSame(pdt, result);
    }

    @Test
    public void shouldReturnRawPrimitivePdtWhenAdapterThrows() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new FailingPrimitiveAdapter());

        final PrimitivePDT pdt = new PrimitivePDT("FailPrim", "42");
        final Object result = registry.hydratePrimitive(pdt);
        assertSame(pdt, result);
    }

    @Test
    public void shouldThrowOnDualRegistrationPrimitiveAfterComposite() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new PointAdapter());

        // Attempt to register Point.class as a primitive (already registered as composite)
        final PrimitivePDTAdapter<Point> primitivePoint = new PrimitivePDTAdapter<Point>() {
            @Override public String typeName() { return "PointPrim"; }
            @Override public Class<Point> targetClass() { return Point.class; }
            @Override public String toValue(Point obj) { return obj.x + "," + obj.y; }
            @Override public Point fromValue(String value) { return new Point(0, 0); }
        };

        try {
            registry.register(primitivePoint);
            fail("Expected IllegalArgumentException for dual registration");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("already registered as a composite"));
        }
    }

    @Test
    public void shouldThrowOnDualRegistrationCompositeAfterPrimitive() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new Uint32Adapter());

        // Attempt to register Uint32.class as a composite (already registered as primitive)
        final CompositePDTAdapter<Uint32> compositeUint32 = new CompositePDTAdapter<Uint32>() {
            @Override public String typeName() { return "Uint32Comp"; }
            @Override public Class<Uint32> targetClass() { return Uint32.class; }
            @Override public Map<String, Object> toFields(Uint32 obj) { return new HashMap<>(); }
            @Override public Uint32 fromFields(Map<String, Object> fields) { return new Uint32(0); }
        };

        try {
            registry.register(compositeUint32);
            fail("Expected IllegalArgumentException for dual registration");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("already registered as a primitive"));
        }
    }

    @Test
    public void shouldHydratePrimitiveNestedInsideComposite() {
        final PDTRegistry registry = PDTRegistry.empty();
        registry.register(new Uint32Adapter());

        // A composite type "Container" with a primitive nested field
        registry.register(new CompositePDTAdapter<Map>() {
            @Override public String typeName() { return "Container"; }
            @Override public Class<Map> targetClass() { return Map.class; }
            @Override public Map<String, Object> toFields(Map obj) { return new HashMap<>(); }
            @Override public Map fromFields(Map<String, Object> fields) { return fields; }
        });

        final Map<String, Object> containerFields = new HashMap<>();
        containerFields.put("id", new PrimitivePDT("Uint32", "99"));
        containerFields.put("label", "test");
        final CompositePDT containerPdt = new CompositePDT("Container", containerFields);

        final Object result = registry.hydrate(containerPdt);
        assertTrue(result instanceof Map);
        final Map<String, Object> resultMap = (Map<String, Object>) result;
        assertTrue(resultMap.get("id") instanceof Uint32);
        assertEquals(99L, ((Uint32) resultMap.get("id")).value);
        assertEquals("test", resultMap.get("label"));
    }
}
