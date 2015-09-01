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
package org.apache.tinkerpop.gremlin.process.computer.bulkloading;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class BulkLoaderVertexProgramTest extends AbstractGremlinProcessTest {

    private BulkLoader getBulkLoader(final BulkLoaderVertexProgram blvp) throws Exception {
        final Field field = BulkLoaderVertexProgram.class.getDeclaredField("bulkLoader");
        field.setAccessible(true);
        return (BulkLoader) field.get(blvp);
    }

    private Graph getTargetGraph() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty(Graph.GRAPH, "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");
        return GraphFactory.open(configuration);
    }

    @Test
    public void shouldUseIncrementalBulkLoaderByDefault() throws Exception {
        final BulkLoader loader = getBulkLoader(BulkLoaderVertexProgram.build().create(graph));
        assertTrue(loader instanceof IncrementalBulkLoader);
        assertTrue(loader.keepOriginalIds());
        assertFalse(loader.useUserSuppliedIds());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldStoreOriginalIds() throws Exception {
        final BulkLoader loader = getBulkLoader(BulkLoaderVertexProgram.build().userSuppliedIds(false).create(graph));
        assertFalse(loader.useUserSuppliedIds());
        final Graph target = getTargetGraph();
        graph.vertices().forEachRemaining(v -> loader.getOrCreateVertex(v, target, target.traversal()));
        target.vertices().forEachRemaining(v -> assertTrue(v.property(loader.getVertexIdProperty()).isPresent()));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotStoreOriginalIds() throws Exception {
        final BulkLoader loader = getBulkLoader(BulkLoaderVertexProgram.build().userSuppliedIds(true).create(graph));
        assertTrue(loader.useUserSuppliedIds());
        final Graph target = getTargetGraph();
        graph.vertices().forEachRemaining(v -> loader.getOrCreateVertex(v, target, target.traversal()));
        target.vertices().forEachRemaining(v -> assertFalse(v.property(loader.getVertexIdProperty()).isPresent()));
    }

    // TODO: once Neo4j supports concurrent connections, write a real integration test that leverages BLVP
}