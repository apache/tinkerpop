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
package org.apache.tinkerpop.machine.functions.filter;

import org.apache.tinkerpop.language.TraversalSource;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HasKeyValueTest {

    private List<TraversalSource<Long>> sources;

    public HasKeyValueTest(final TraversalSource<Long>... sources) {
        this.sources = Arrays.asList(sources);
    }

    @Test
    public void g_injectXa_1__b_2X_hasXa_1X() {
        for (final TraversalSource<Long> g : this.sources) {
            final Map<String, Integer> map = Map.of("a", 1, "b", 2);
            final List<Map<String, Integer>> list = g.inject(map).has("a", 1).toList();
            assertEquals(1, list.size());
            assertEquals(map, list.get(0));
        }
    }

    @Test
    public void g_injectXa_a_a_bX_groupCount_hasXa_1X() {
        for (final TraversalSource<Long> g : this.sources) {
            assertFalse(g.inject("a", "a", "a", "b").groupCount().has("a", 1L).hasNext());
        }
    }

}
