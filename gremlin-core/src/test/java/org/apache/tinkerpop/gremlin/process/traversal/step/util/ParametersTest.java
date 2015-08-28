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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ParametersTest {
    @Test
    public void shouldGetRaw() {
        final Parameters parameters = new Parameters();
        parameters.set("a", "axe", "b", "bat", "c", "cat");

        final Map<Object,Object> params = parameters.getRaw();
        assertEquals(3, params.size());
        assertEquals("axe", params.get("a"));
        assertEquals("bat", params.get("b"));
        assertEquals("cat", params.get("c"));
    }

    @Test
    public void shouldGetRawEmptyAndUnmodifiable() {
        final Parameters parameters = new Parameters();
        final Map<Object,Object> params = parameters.getRaw();
        assertEquals(Collections.emptyMap(), params);
    }

    @Test
    public void shouldGetRawExcept() {
        final Parameters parameters = new Parameters();
        parameters.set("a", "axe", "b", "bat", "c", "cat");

        final Map<Object,Object> params = parameters.getRaw("b");
        assertEquals(2, params.size());
        assertEquals("axe", params.get("a"));
        assertEquals("cat", params.get("c"));
    }

    @Test
    public void shouldRemove() {
        final Parameters parameters = new Parameters();
        parameters.set("a", "axe", "b", "bat", "c", "cat");

        final Map<Object,Object> before = parameters.getRaw();
        assertEquals(3, before.size());
        assertEquals("axe", before.get("a"));
        assertEquals("bat", before.get("b"));
        assertEquals("cat", before.get("c"));

        parameters.remove("b");

        final Map<Object,Object> after = parameters.getRaw("b");
        assertEquals(2, after.size());
        assertEquals("axe", after.get("a"));
        assertEquals("cat", after.get("c"));
    }
}
