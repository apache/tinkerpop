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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests that support the creation of {@link org.apache.tinkerpop.gremlin.structure.Graph} instances which occurs via
 * {@link org.apache.tinkerpop.gremlin.structure.util.GraphFactory}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphConstructionTest extends AbstractGremlinTest {
    /**
     * All Gremlin Structure implementations should be constructable through
     * {@link org.apache.tinkerpop.gremlin.structure.util.GraphFactory}.
     */
    @Test
    public void shouldOpenGraphThroughGraphFactoryViaApacheConfig() throws Exception {
        final Graph expectedGraph = graph;
        final Configuration c = graphProvider.newGraphConfiguration("temp1", this.getClass(), name.getMethodName(), null);
        final Graph createdGraph = GraphFactory.open(c);

        assertNotNull(createdGraph);
        assertEquals(expectedGraph.getClass(), createdGraph.getClass());

        graphProvider.clear(createdGraph, c);
    }

    /**
     * Graphs should be empty on creation.
     */
    @Test
    public void shouldConstructAnEmptyGraph() {
        assertVertexEdgeCounts(graph, 0, 0);
    }

    /**
     * A {@link Graph} should maintain the original {@code Configuration} object passed to it via {@link GraphFactory}.
     */
    @Test
    public void shouldMaintainOriginalConfigurationObjectGivenToFactory() throws Exception {
        final Configuration originalConfig = graphProvider.newGraphConfiguration("temp2", this.getClass(), name.getMethodName(), null);
        final Graph createdGraph = GraphFactory.open(originalConfig);

        final Configuration configInGraph = createdGraph.configuration();
        final AtomicInteger keyCount = new AtomicInteger(0);
        originalConfig.getKeys().forEachRemaining(k -> {
            assertTrue(configInGraph.containsKey(k));
            keyCount.incrementAndGet();
        });

        // need some keys in the originalConfig for this test to be meaningful
        assertTrue(keyCount.get() > 0);
        assertEquals(keyCount.get(), IteratorUtils.count(configInGraph.getKeys()));

        graphProvider.clear(createdGraph, originalConfig);
    }
}
