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
package org.apache.tinkerpop.gremlin.util.tools;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MultiMapTest {

    @Test
    public void shouldBeUtilityClass() throws Exception {
        TestHelper.assertIsUtilityClass(MultiMap.class);
    }

    @Test
    public void shouldPutValueInNewSet() {
        final Map<String,Set<Object>> multi = new HashMap<>();
        MultiMap.put(multi, "here", 1);
        MultiMap.put(multi, "there", 2);

        final Set<Object> expected = new HashSet<Object>() {{
            add(1);
        }};
        assertEquals(expected, multi.get("here"));
    }

    @Test
    public void shouldPutValueInExistingSet() {
        final Map<String,Set<Object>> multi = new HashMap<>();
        MultiMap.put(multi, "here", 1);
        MultiMap.put(multi, "there", 3);

        final Set<Object> expected1 = new HashSet<Object>() {{
            add(1);
        }};
        assertEquals(expected1, multi.get("here"));

        MultiMap.put(multi, "here", 2);

        final Set<Object> expected2 = new HashSet<Object>() {{
            add(1);
            add(2);
        }};
        assertEquals(expected2, multi.get("here"));
    }

    @Test
    public void shouldPutCollectionInSet() {
        final Map<String,Set<Object>> multi = new HashMap<>();
        final Set<Object> expected = new HashSet<Object>() {{
            add(1);
            add(2);
        }};
        MultiMap.putAll(multi, "here", expected);
        assertEquals(expected, multi.get("here"));
    }

    @Test
    public void shouldContainEntry() {
        final Map<String,Set<Object>> multi = new HashMap<>();
        MultiMap.put(multi, "here", 1);
        MultiMap.put(multi, "there", 2);
        MultiMap.put(multi, "here", 2);

        assertThat(MultiMap.containsEntry(multi, "here", 1), is(true));
    }

    @Test
    public void shouldNotContainEntryValue() {
        final Map<String,Set<Object>> multi = new HashMap<>();
        MultiMap.put(multi, "here", 1);
        MultiMap.put(multi, "there", 2);

        assertThat(MultiMap.containsEntry(multi, "here", 2), is(false));
    }

    @Test
    public void shouldNotContainEntryKey() {
        final Map<String,Set<Object>> multi = new HashMap<>();
        MultiMap.put(multi, "here", 1);
        MultiMap.put(multi, "there", 2);

        assertThat(MultiMap.containsEntry(multi, "that", 2), is(false));
    }

    @Test
    public void shouldGetAnEntry() {
        final Map<String,Set<Object>> multi = new HashMap<>();
        MultiMap.put(multi, "here", 1);
        MultiMap.put(multi, "there", 3);

        final Set<Object> expected1 = new HashSet<Object>() {{
            add(1);
        }};
        assertEquals(expected1, MultiMap.get(multi, "here"));

        MultiMap.put(multi, "here", 2);

        final Set<Object> expected2 = new HashSet<Object>() {{
            add(1);
            add(2);
        }};
        assertEquals(expected2, MultiMap.get(multi, "here"));
    }

    @Test
    public void shouldGetEmptyIfKeyNotPresent() {
        final Map<String,Set<Object>> multi = new HashMap<>();
        MultiMap.put(multi, "here", 1);
        MultiMap.put(multi, "there", 3);

        assertEquals(Collections.emptySet(), MultiMap.get(multi, "not-here"));
    }
}
