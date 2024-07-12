/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.util;

import org.apache.tinkerpop.gremlin.AssertHelper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CollectionUtilTest {

    @Test
    public void shouldBeUtilityClass() throws Exception {
        AssertHelper.assertIsUtilityClass(CollectionUtil.class);
    }

    @Test
    public void shouldCloneConcurrentHashMapWithPrimitiveValues() {
        final ConcurrentHashMap<Object, String> source = new ConcurrentHashMap<>();
        source.put("key", "value");
        source.put(1, "test");

        final ConcurrentHashMap<?,?> cloned = CollectionUtil.clone(source);
        assertTrue(source.equals(cloned));
    }

    @Test
    public void shouldCloneConcurrentHashMapWithArrayListValues() {
        final ConcurrentHashMap<String, ArrayList<String>> source = new ConcurrentHashMap<>();
        final ArrayList<String> value = new ArrayList<>();
        value.add("test1");
        value.add("test2");
        source.put("key", value);
        source.put("empty", new ArrayList<>());

        final ConcurrentHashMap<?, ?> cloned = CollectionUtil.clone(source);
        assertTrue(source.equals(cloned));
    }

    @Test
    public void shouldCloneConcurrentHashMapWithSetValues() {
        final ConcurrentHashMap<String, Set<String>> source = new ConcurrentHashMap<>();
        final HashSet<String> value = new HashSet<>();
        value.add("test1");
        value.add("test2");
        source.put("key", value);
        source.put("empty", new HashSet<>());

        final ConcurrentHashMap<?, ?> cloned = CollectionUtil.clone(source);
        assertTrue(source.equals(cloned));
    }

    @Test
    public void shouldCloneEmptyConcurrentHashMap() {
        final ConcurrentHashMap<String, String> source = new ConcurrentHashMap<>();
        final ConcurrentHashMap<?, ?> cloned = CollectionUtil.clone(source);
        assertTrue(source.equals(cloned));
    }

    @Test
    public void shouldCloneConcurrentHashMapWithMixedTypes() {
        final ConcurrentHashMap<String, Object> source = new ConcurrentHashMap<>();
        source.put("key1", "value1");
        source.put("key2", new ArrayList<>(Arrays.asList("a", "b")));
        source.put("key3", new HashSet<>(Arrays.asList("x", "y")));

        final ConcurrentHashMap<?, ?> cloned = CollectionUtil.clone(source);
        assertTrue(source.equals(cloned));
    }

    @Test
    public void shouldAddFirstWhenBothArgumentsNull() {
        String[] result = CollectionUtil.addFirst(null, null, String.class);
        assertArrayEquals(new String[]{null}, result);
    }

    @Test
    public void shouldAddFirstWhenArrayNull() {
        String[] result = CollectionUtil.addFirst(null, "element", String.class);
        assertArrayEquals(new String[]{"element"}, result);
    }

    @Test
    public void shoulAddFirstWhenNeitherArgumentNull() {
        Integer[] array = {1, 2, 3};
        Integer[] result = CollectionUtil.addFirst(array, 0, Integer.class);
        assertArrayEquals(new Integer[]{0, 1, 2, 3}, result);
    }

    @Test
    public void shouldAddFirstWhenEmptyArray() {
        String[] array = {};
        String[] result = CollectionUtil.addFirst(array, "element", String.class);
        assertArrayEquals(new String[]{"element"}, result);
    }

    @Test
    public void shouldAddFirstWhenIntegers() {
        Integer[] array = {1, 2, 3};
        Integer[] result = CollectionUtil.addFirst(array, 0, Integer.class);
        assertArrayEquals(new Integer[]{0, 1, 2, 3}, result);
    }

    @Test
    public void shouldConvertVarargsToList() {
        List<String> result = CollectionUtil.asList("a", "b", "c");
        assertEquals(Arrays.asList("a", "b", "c"), result);
    }

    @Test
    public void shouldConvertEmptyVarargsToList() {
        List<String> result = CollectionUtil.asList();
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void shouldConvertVarargsToSet() {
        Set<String> result = CollectionUtil.asSet("a", "b", "c");
        assertEquals(new LinkedHashSet<>(Arrays.asList("a", "b", "c")), result);
    }

    @Test
    public void shouldConvertEmptyVarargsToSet() {
        Set<String> result = CollectionUtil.asSet();
        assertEquals(new LinkedHashSet<>(), result);
    }

    @Test
    public void shouldConvertCollectionToSet() {
        Collection<String> collection = Arrays.asList("a", "b", "c");
        Set<String> result = CollectionUtil.asSet(collection);
        assertEquals(new LinkedHashSet<>(collection), result);
    }

    @Test
    public void shouldConvertVarargsToMap() {
        Map<String, String> result = CollectionUtil.asMap("key1", "value1", "key2", "value2");
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("key1", "value1");
        expected.put("key2", "value2");
        assertEquals(expected, result);
    }

    @Test
    public void shouldConvertVarargsToMapWithOddNumberOfElements() {
        Map<String, String> result = CollectionUtil.asMap("key1", "value1", "key2");
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("key1", "value1");
        expected.put("key2", null);
        assertEquals(expected, result);
    }
}
