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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
}
