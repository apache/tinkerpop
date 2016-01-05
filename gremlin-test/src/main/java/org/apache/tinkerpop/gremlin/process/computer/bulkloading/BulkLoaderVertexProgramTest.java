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
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class BulkLoaderVertexProgramTest extends AbstractGremlinProcessTest {

    final static String TINKERGRAPH_LOCATION = TestHelper.makeTestDataDirectory(BulkLoaderVertexProgramTest.class) + "tinkertest.kryo";

    private BulkLoader getBulkLoader(final BulkLoaderVertexProgram blvp) throws Exception {
        final Field field = BulkLoaderVertexProgram.class.getDeclaredField("bulkLoader");
        field.setAccessible(true);
        return (BulkLoader) field.get(blvp);
    }

    private Configuration getWriteGraphConfiguration() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty(Graph.GRAPH, "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");
        configuration.setProperty("gremlin.tinkergraph.graphLocation", TINKERGRAPH_LOCATION);
        configuration.setProperty("gremlin.tinkergraph.graphFormat", "gryo");
        return configuration;
    }

    private Graph getWriteGraph() {
        return GraphFactory.open(getWriteGraphConfiguration());
    }

    @After
    public void cleanup() {
        final File graph = new File(TINKERGRAPH_LOCATION);
        assertTrue(!graph.exists() || graph.delete());
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
        final BulkLoaderVertexProgram blvp = BulkLoaderVertexProgram.build()
                .userSuppliedIds(false)
                .writeGraph(getWriteGraphConfiguration()).create(graph);
        final BulkLoader loader = getBulkLoader(blvp);
        assertFalse(loader.useUserSuppliedIds());
        graph.compute(graphComputerClass.get()).workers(1).program(blvp).submit().get();
        assertGraphEquality(graph, getWriteGraph(), v -> v.value(loader.getVertexIdProperty()));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotStoreOriginalIds() throws Exception {
        final BulkLoaderVertexProgram blvp = BulkLoaderVertexProgram.build()
                .userSuppliedIds(true)
                .writeGraph(getWriteGraphConfiguration()).create(graph);
        final BulkLoader loader = getBulkLoader(blvp);
        assertTrue(loader.useUserSuppliedIds());
        graph.compute(graphComputerClass.get()).workers(1).program(blvp).submit().get();
        assertGraphEquality(graph, getWriteGraph());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldOverwriteExistingElements() throws Exception {
        final BulkLoaderVertexProgram blvp = BulkLoaderVertexProgram.build()
                .userSuppliedIds(true)
                .writeGraph(getWriteGraphConfiguration()).create(graph);
        graph.compute(graphComputerClass.get()).workers(1).program(blvp).submit().get(); // initial
        graph.compute(graphComputerClass.get()).workers(1).program(blvp).submit().get(); // incremental
        assertGraphEquality(graph, getWriteGraph());
    }

    @Test
    @LoadGraphWith(MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER) // we can't modify the graph in computer mode
    public void shouldProperlyHandleMetaProperties() throws Exception {
        graph.traversal().V().has("name", "marko").properties("name").property("alias", "okram").iterate();
        final BulkLoaderVertexProgram blvp = BulkLoaderVertexProgram.build()
                .userSuppliedIds(true)
                .writeGraph(getWriteGraphConfiguration()).create(graph);
        graph.compute(graphComputerClass.get()).workers(1).program(blvp).submit().get();
        assertGraphEquality(graph, getWriteGraph());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldUseOneTimeBulkLoader() throws Exception {
        for (int iteration = 1; iteration <= 2; iteration++) {
            final BulkLoaderVertexProgram blvp = BulkLoaderVertexProgram.build()
                    .bulkLoader(OneTimeBulkLoader.class)
                    .writeGraph(getWriteGraphConfiguration()).create(graph);
            final BulkLoader loader = getBulkLoader(blvp);
            assertTrue(loader instanceof OneTimeBulkLoader);
            graph.compute(graphComputerClass.get()).workers(1).program(blvp).submit().get();
            final Graph result = getWriteGraph();
            assertEquals(6 * iteration, IteratorUtils.count(result.vertices()));
            assertEquals(6 * iteration, IteratorUtils.count(result.edges()));
            result.close();
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldUseOneTimeBulkLoaderWithUserSuppliedIds() throws Exception {
        final BulkLoaderVertexProgram blvp = BulkLoaderVertexProgram.build()
                .bulkLoader(OneTimeBulkLoader.class)
                .userSuppliedIds(true)
                .writeGraph(getWriteGraphConfiguration()).create(graph);
        final BulkLoader loader = getBulkLoader(blvp);
        assertTrue(loader instanceof OneTimeBulkLoader);
        graph.compute(graphComputerClass.get()).workers(1).program(blvp).submit().get();
        final Graph result = getWriteGraph();
        assertEquals(6, IteratorUtils.count(result.vertices()));
        assertEquals(6, IteratorUtils.count(result.edges()));
        result.close();
    }

    private static void assertGraphEquality(final Graph source, final Graph target) {
        assertGraphEquality(source, target, Element::id);
    }

    private static void assertGraphEquality(final Graph source, final Graph target, final Function<Vertex, Object> idAccessor) {
        final GraphTraversalSource tg = target.traversal();
        assertEquals(IteratorUtils.count(source.vertices()), IteratorUtils.count(target.vertices()));
        assertEquals(IteratorUtils.count(target.edges()), IteratorUtils.count(target.edges()));
        source.vertices().forEachRemaining(originalVertex -> {
            Vertex tmpVertex = null;
            final Iterator<Vertex> vertexIterator = target.vertices();
            while (vertexIterator.hasNext()) {
                final Vertex v = vertexIterator.next();
                if (idAccessor.apply(v).toString().equals(originalVertex.id().toString())) {
                    tmpVertex = v;
                    break;
                }
            }
            assertNotNull(tmpVertex);
            final Vertex clonedVertex = tmpVertex;
            assertEquals(IteratorUtils.count(originalVertex.edges(Direction.IN)), IteratorUtils.count(clonedVertex.edges(Direction.IN)));
            assertEquals(IteratorUtils.count(originalVertex.edges(Direction.OUT)), IteratorUtils.count(clonedVertex.edges(Direction.OUT)));
            assertEquals(originalVertex.label(), clonedVertex.label());
            originalVertex.properties().forEachRemaining(originalProperty -> {
                VertexProperty clonedProperty = null;
                final Iterator<VertexProperty<Object>> vertexPropertyIterator = clonedVertex.properties(originalProperty.key());
                while (vertexPropertyIterator.hasNext()) {
                    final VertexProperty p = vertexPropertyIterator.next();
                    if (p.value().equals(originalProperty.value())) {
                        clonedProperty = p;
                        break;
                    }
                }
                assertNotNull(clonedProperty);
                assertEquals(originalProperty.isPresent(), clonedProperty.isPresent());
                assertEquals(originalProperty.value(), clonedProperty.value());
            });
            originalVertex.edges(Direction.OUT).forEachRemaining(originalEdge -> {
                GraphTraversal t = tg.V(clonedVertex).outE(originalEdge.label());
                originalEdge.properties().forEachRemaining(p -> t.has(p.key(), p.value()));
                assertTrue(t.hasNext());
            });
        });
    }
}