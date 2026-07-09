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

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

public class CompositePDTTest {

    @ProviderDefined
    static class Point {
        int x = 1;
        int y = 2;
    }

    @ProviderDefined(name = "GeoPoint")
    static class NamedPoint {
        double lat = 45.0;
        double lon = -93.0;
    }

    @ProviderDefined(includedFields = {"x"})
    static class IncludedFieldsPoint {
        int x = 10;
        int y = 20;
        int z = 30;
    }

    @ProviderDefined(excludedFields = {"z"})
    static class ExcludedFieldsPoint {
        int x = 10;
        int y = 20;
        int z = 30;
    }

    @ProviderDefined(includedFields = {"x"}, excludedFields = {"z"})
    static class ConflictingFieldsPoint {
        int x = 10;
        int y = 20;
        int z = 30;
    }

    @ProviderDefined
    static class NullFieldPoint {
        String label = null;
        int x = 5;
    }

    static class NotAnnotated {
        int value = 1;
    }

    static class BasePoint {
        int x = 1;
        int y = 2;
    }

    @ProviderDefined(name = "GeoPoint")
    static class InheritedPoint extends BasePoint {
        String label = "origin";
    }

    @ProviderDefined(excludedFields = {"y"})
    static class InheritedExcluded extends BasePoint {
        String label = "test";
    }

    @ProviderDefined(includedFields = {"x", "label"})
    static class InheritedIncluded extends BasePoint {
        String label = "included";
    }

    @Test
    public void shouldConstructDirectly() {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 1);
        fields.put("y", 2);
        final CompositePDT pdt = new CompositePDT("Point", fields);
        assertEquals("Point", pdt.getName());
        assertEquals(fields, pdt.getFields());
    }

    @Test
    public void shouldBeImmutableFromInputMap() {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 1);
        final CompositePDT pdt = new CompositePDT("Point", fields);
        fields.put("y", 2);
        assertEquals(1, pdt.getFields().size());
    }

    @Test
    public void shouldReturnUnmodifiableFields() {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 1);
        final CompositePDT pdt = new CompositePDT("Point", fields);
        assertThrows(UnsupportedOperationException.class, () -> pdt.getFields().put("y", 2));
    }

    @Test
    public void shouldCreateFromAnnotatedObject() {
        final CompositePDT pdt = CompositePDT.from(new Point());
        assertEquals("Point", pdt.getName());
        assertEquals(1, pdt.getFields().get("x"));
        assertEquals(2, pdt.getFields().get("y"));
    }

    @Test
    public void shouldUseCustomNameFromAnnotation() {
        final CompositePDT pdt = CompositePDT.from(new NamedPoint());
        assertEquals("GeoPoint", pdt.getName());
    }

    @Test
    public void shouldFilterWithIncludedFields() {
        final CompositePDT pdt = CompositePDT.from(new IncludedFieldsPoint());
        assertEquals(1, pdt.getFields().size());
        assertEquals(10, pdt.getFields().get("x"));
    }

    @Test
    public void shouldFilterWithExcludedFields() {
        final CompositePDT pdt = CompositePDT.from(new ExcludedFieldsPoint());
        assertEquals(2, pdt.getFields().size());
        assertEquals(10, pdt.getFields().get("x"));
        assertEquals(20, pdt.getFields().get("y"));
    }

    @Test
    public void shouldThrowOnNullObject() {
        assertThrows(IllegalArgumentException.class, () -> CompositePDT.from(null));
    }

    @Test
    public void shouldThrowOnNonAnnotatedObject() {
        assertThrows(IllegalArgumentException.class, () -> CompositePDT.from(new NotAnnotated()));
    }

    @Test
    public void shouldHaveCorrectEqualsAndHashCode() {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("x", 1);
        final CompositePDT a = new CompositePDT("Point", fields);
        final CompositePDT b = new CompositePDT("Point", fields);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        final CompositePDT c = new CompositePDT("Other", fields);
        assertNotEquals(a, c);
    }

    @Test
    public void shouldThrowOnNullName() {
        assertThrows(IllegalArgumentException.class, () -> new CompositePDT(null, new HashMap<>()));
    }

    @Test
    public void shouldThrowOnEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> new CompositePDT("", new HashMap<>()));
    }

    @Test
    public void shouldThrowOnNullFields() {
        assertThrows(IllegalArgumentException.class, () -> new CompositePDT("Point", null));
    }

    @Test
    public void shouldPreserveNullFieldValues() {
        final CompositePDT pdt = CompositePDT.from(new NullFieldPoint());
        assertEquals(2, pdt.getFields().size());
        assertEquals(null, pdt.getFields().get("label"));
        assertEquals(5, pdt.getFields().get("x"));
    }

    @Test
    public void shouldThrowOnConflictingIncludedAndExcludedFields() {
        assertThrows(IllegalArgumentException.class, () -> CompositePDT.from(new ConflictingFieldsPoint()));
    }

    @Test
    public void shouldIncludeInheritedFields() {
        final CompositePDT pdt = CompositePDT.from(new InheritedPoint());
        assertEquals("GeoPoint", pdt.getName());
        assertEquals(3, pdt.getFields().size());
        assertEquals("origin", pdt.getFields().get("label"));
        assertEquals(1, pdt.getFields().get("x"));
        assertEquals(2, pdt.getFields().get("y"));
    }

    @Test
    public void shouldExcludeInheritedFields() {
        final CompositePDT pdt = CompositePDT.from(new InheritedExcluded());
        assertEquals(2, pdt.getFields().size());
        assertEquals("test", pdt.getFields().get("label"));
        assertEquals(1, pdt.getFields().get("x"));
    }

    @Test
    public void shouldIncludeOnlySpecifiedFieldsAcrossHierarchy() {
        final CompositePDT pdt = CompositePDT.from(new InheritedIncluded());
        assertEquals(2, pdt.getFields().size());
        assertEquals("included", pdt.getFields().get("label"));
        assertEquals(1, pdt.getFields().get("x"));
    }
}
