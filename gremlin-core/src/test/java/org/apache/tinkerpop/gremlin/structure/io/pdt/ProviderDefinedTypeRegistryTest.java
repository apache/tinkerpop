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

public class ProviderDefinedTypeRegistryTest {

    // Simple test type
    static class Point {
        final int x;
        final int y;
        Point(int x, int y) { this.x = x; this.y = y; }
    }

    static class PointAdapter implements ProviderDefinedTypeAdapter<Point> {
        @Override public String typeName() { return "Point"; }
        @Override public Class<Point> targetClass() { return Point.class; }
        @Override public Map<String, Object> toProperties(Point obj) {
            final Map<String, Object> m = new HashMap<>();
            m.put("x", obj.x);
            m.put("y", obj.y);
            return m;
        }
        @Override public Point fromProperties(Map<String, Object> properties) {
            return new Point((int) properties.get("x"), (int) properties.get("y"));
        }
    }

    // Nested test type
    static class Line {
        final Point start;
        final Point end;
        Line(Point start, Point end) { this.start = start; this.end = end; }
    }

    static class LineAdapter implements ProviderDefinedTypeAdapter<Line> {
        @Override public String typeName() { return "Line"; }
        @Override public Class<Line> targetClass() { return Line.class; }
        @Override public Map<String, Object> toProperties(Line obj) {
            final Map<String, Object> m = new HashMap<>();
            m.put("start", obj.start);
            m.put("end", obj.end);
            return m;
        }
        @Override public Line fromProperties(Map<String, Object> properties) {
            return new Line((Point) properties.get("start"), (Point) properties.get("end"));
        }
    }

    // Adapter that always throws
    static class FailingAdapter implements ProviderDefinedTypeAdapter<Point> {
        @Override public String typeName() { return "Failing"; }
        @Override public Class<Point> targetClass() { return Point.class; }
        @Override public Map<String, Object> toProperties(Point obj) { return new HashMap<>(); }
        @Override public Point fromProperties(Map<String, Object> properties) {
            throw new RuntimeException("intentional failure");
        }
    }

    @Test
    public void shouldHydrateSimplePdt() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new PointAdapter());

        final Map<String, Object> props = new HashMap<>();
        props.put("x", 3);
        props.put("y", 7);
        final ProviderDefinedType pdt = new ProviderDefinedType("Point", props);

        final Object result = registry.hydrate(pdt);
        assertTrue(result instanceof Point);
        assertEquals(3, ((Point) result).x);
        assertEquals(7, ((Point) result).y);
    }

    @Test
    public void shouldReturnRawPdtWhenNoAdapterRegistered() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();

        final Map<String, Object> props = new HashMap<>();
        props.put("x", 1);
        final ProviderDefinedType pdt = new ProviderDefinedType("Unknown", props);

        final Object result = registry.hydrate(pdt);
        assertSame(pdt, result);
    }

    @Test
    public void shouldHydrateNestedPdts() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new PointAdapter());
        registry.register(new LineAdapter());

        final Map<String, Object> startProps = new HashMap<>();
        startProps.put("x", 0);
        startProps.put("y", 0);
        final Map<String, Object> endProps = new HashMap<>();
        endProps.put("x", 5);
        endProps.put("y", 5);

        final Map<String, Object> lineProps = new HashMap<>();
        lineProps.put("start", new ProviderDefinedType("Point", startProps));
        lineProps.put("end", new ProviderDefinedType("Point", endProps));
        final ProviderDefinedType linePdt = new ProviderDefinedType("Line", lineProps);

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
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new LineAdapter());
        // Point adapter NOT registered

        final Map<String, Object> startProps = new HashMap<>();
        startProps.put("x", 1);
        startProps.put("y", 2);
        final ProviderDefinedType startPdt = new ProviderDefinedType("Point", startProps);

        final Map<String, Object> endProps = new HashMap<>();
        endProps.put("x", 3);
        endProps.put("y", 4);
        final ProviderDefinedType endPdt = new ProviderDefinedType("Point", endProps);

        final Map<String, Object> lineProps = new HashMap<>();
        lineProps.put("start", startPdt);
        lineProps.put("end", endPdt);
        final ProviderDefinedType linePdt = new ProviderDefinedType("Line", lineProps);

        // Line adapter will receive ProviderDefinedType values for start/end since Point is not registered.
        // The LineAdapter.fromProperties casts to Point which will throw ClassCastException,
        // so hydrate should fall back to returning the raw PDT.
        final Object result = registry.hydrate(linePdt);
        assertSame(linePdt, result);
    }

    @Test
    public void shouldFallBackWhenAdapterThrows() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new FailingAdapter());

        final Map<String, Object> props = new HashMap<>();
        props.put("x", 1);
        final ProviderDefinedType pdt = new ProviderDefinedType("Failing", props);

        // should not throw, should return raw PDT
        final Object result = registry.hydrate(pdt);
        assertSame(pdt, result);
    }

    @Test
    public void shouldLookUpAdapterByClass() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        final PointAdapter adapter = new PointAdapter();
        registry.register(adapter);

        final Optional<ProviderDefinedTypeAdapter<?>> found = registry.getAdapterByClass(Point.class);
        assertTrue(found.isPresent());
        assertEquals("Point", found.get().typeName());
    }

    // Collection test type
    static class Polygon {
        final List<Point> vertices;
        Polygon(List<Point> vertices) { this.vertices = vertices; }
    }

    static class PolygonAdapter implements ProviderDefinedTypeAdapter<Polygon> {
        @Override public String typeName() { return "Polygon"; }
        @Override public Class<Polygon> targetClass() { return Polygon.class; }
        @Override public Map<String, Object> toProperties(Polygon obj) {
            final Map<String, Object> m = new HashMap<>();
            m.put("vertices", obj.vertices);
            return m;
        }
        @SuppressWarnings("unchecked")
        @Override public Polygon fromProperties(Map<String, Object> properties) {
            return new Polygon((List<Point>) properties.get("vertices"));
        }
    }

    @Test
    public void shouldHydratePdtsInsideList() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new PointAdapter());
        registry.register(new PolygonAdapter());

        final Map<String, Object> p1 = new HashMap<>();
        p1.put("x", 1); p1.put("y", 2);
        final Map<String, Object> p2 = new HashMap<>();
        p2.put("x", 3); p2.put("y", 4);

        final Map<String, Object> polyProps = new HashMap<>();
        polyProps.put("vertices", Arrays.asList(
                new ProviderDefinedType("Point", p1),
                new ProviderDefinedType("Point", p2)));
        final ProviderDefinedType polyPdt = new ProviderDefinedType("Polygon", polyProps);

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
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(new PointAdapter());

        // A simple adapter that receives a map of named points
        registry.register(new ProviderDefinedTypeAdapter<Map>() {
            @Override public String typeName() { return "PointMap"; }
            @Override public Class<Map> targetClass() { return Map.class; }
            @Override public Map<String, Object> toProperties(Map obj) { return new HashMap<>(); }
            @SuppressWarnings("unchecked")
            @Override public Map fromProperties(Map<String, Object> properties) {
                return (Map<String, Object>) properties.get("points");
            }
        });

        final Map<String, Object> p1 = new HashMap<>();
        p1.put("x", 10); p1.put("y", 20);
        final Map<String, Object> p2 = new HashMap<>();
        p2.put("x", 30); p2.put("y", 40);

        final Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("origin", new ProviderDefinedType("Point", p1));
        innerMap.put("target", new ProviderDefinedType("Point", p2));

        final Map<String, Object> props = new HashMap<>();
        props.put("points", innerMap);
        final ProviderDefinedType pdt = new ProviderDefinedType("PointMap", props);

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
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.build();

        final Map<String, Object> props = new HashMap<>();
        props.put("x", 1);
        final ProviderDefinedType pdt = new ProviderDefinedType("Unregistered", props);
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
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(AnnotatedPoint.class);

        final Map<String, Object> props = new HashMap<>();
        props.put("x", 3);
        props.put("y", 7);
        final Object result = registry.hydrate(new ProviderDefinedType("AnnotatedPoint", props));

        assertTrue(result instanceof AnnotatedPoint);
        assertEquals(3, ((AnnotatedPoint) result).x);
        assertEquals(7, ((AnnotatedPoint) result).y);
    }

    @Test
    public void shouldDehydrateAnnotatedClassViaAdapter() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(AnnotatedPoint.class);

        final Optional<ProviderDefinedTypeAdapter<?>> adapter = registry.getAdapterByClass(AnnotatedPoint.class);
        assertTrue(adapter.isPresent());
        assertEquals("AnnotatedPoint", adapter.get().typeName());
    }

    @Test
    public void shouldRespectExcludedFieldsWhenHydratingAnnotatedClass() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        registry.register(ExcludedFields.class);

        final Map<String, Object> props = new HashMap<>();
        props.put("value", 42);
        props.put("secret", "should-be-ignored");
        final Object result = registry.hydrate(new ProviderDefinedType("Excluded", props));

        assertTrue(result instanceof ExcludedFields);
        assertEquals(42, ((ExcludedFields) result).value);
        // secret is excluded from the field mapping, so it is not set
        assertEquals(null, ((ExcludedFields) result).secret);
    }

    @Test
    public void shouldThrowWhenRegisteringNonAnnotatedClass() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        try {
            registry.register(NotAnnotated.class);
            fail("Expected IllegalArgumentException for non-annotated class");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("not annotated with @ProviderDefined"));
        }
    }

    @Test
    public void shouldThrowWhenRegisteringClassWithoutNoArgConstructor() {
        final ProviderDefinedTypeRegistry registry = ProviderDefinedTypeRegistry.empty();
        try {
            registry.register(NoNoArgCtor.class);
            fail("Expected IllegalArgumentException for class without no-arg constructor");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("no-arg constructor"));
        }
    }
}
