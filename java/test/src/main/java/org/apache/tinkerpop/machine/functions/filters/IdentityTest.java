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
package org.apache.tinkerpop.machine.functions.filters;

import org.apache.tinkerpop.language.TraversalSource;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class IdentityTest {

    private List<TraversalSource<Long>> sources;

    public IdentityTest(final TraversalSource<Long>... sources) {
        this.sources = Arrays.asList(sources);
    }

    @Test
    public void g_injectX2X_identity() {
        for (final TraversalSource<Long> g : this.sources) {
            final List<Long> list = g.inject(2L).identity().toList();
            assertEquals(1, list.size());
            assertEquals(2L, list.get(0));
        }
    }

    @Test
    public void g_injectX1_2X_identity_asXaX() {
        for (final TraversalSource<Long> g : this.sources) {
            final List<Long> list = g.inject(1L, 2L).identity().as("a").toList();
            assertEquals(2, list.size());
            assertTrue(list.contains(1L));
            assertTrue(list.contains(2L));
        }
    }

    @Test
    public void g_injectX1_2X_cX3X_identity() {
        for (final TraversalSource<Long> g : this.sources) {
            final List<Long> list = g.inject(1L, 2L).c(3L).identity().toList();
            assertEquals(6, list.size());
            assertTrue(list.contains(1L));
            assertTrue(list.contains(2L));
        }
    }
}
