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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerStorageGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Engine-agnostic conformance suite ("TCK") for pluggable {@link TinkerStorage} engines. A concrete engine is tested
 * by subclassing this and returning the {@code gremlin.tinkergraph.storage} value that selects it (an engine name or a
 * fully-qualified class name). Every test opens a {@link TinkerStorageGraph} backed by a fresh temporary directory,
 * mutates it, reopens from the same configuration, and asserts the data survived. A new engine drops in by adding one
 * subclass.
 */
public abstract class AbstractTinkerStorageConformanceTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String location;

    /**
     * The {@code gremlin.tinkergraph.storage} configuration value that selects the engine under test.
     */
    protected abstract String storageEngine();

    @Before
    public void setUp() throws Exception {
        location = tempFolder.newFolder("storage").getAbsolutePath();
    }

    protected Configuration config() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinkerStorageGraph.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, storageEngine());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, location);
        return conf;
    }

    protected TinkerStorageGraph open() {
        return TinkerStorageGraph.open(config());
    }

    @Test
    public void shouldPersistVerticesAndEdgesAcrossReopen() {
        TinkerStorageGraph graph = open();
        final Vertex marko = graph.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29);
        final Vertex lop = graph.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4);
        graph.tx().commit();
        graph.close();

        graph = open();
        assertEquals(2, countOf(graph.vertices()));
        assertEquals(1, countOf(graph.edges()));
        final Vertex reMarko = graph.vertices(1).next();
        assertEquals("marko", reMarko.value("name"));
        assertEquals(Integer.valueOf(29), reMarko.value("age"));
        final Edge reCreated = graph.edges(9).next();
        assertEquals("created", reCreated.label());
        assertEquals(0.4, reCreated.<Double>value("weight"), 0.0001);
        assertEquals(Integer.valueOf(1), reCreated.outVertex().id());
        assertEquals(Integer.valueOf(3), reCreated.inVertex().id());
        graph.close();
    }

    @Test
    public void shouldPersistAcrossMultipleCommits() {
        TinkerStorageGraph graph = open();
        for (int i = 0; i < 10; i++) {
            graph.addVertex(T.id, i, "value", i);
            graph.tx().commit();
        }
        graph.close();

        graph = open();
        assertEquals(10, countOf(graph.vertices()));
        for (int i = 0; i < 10; i++)
            assertEquals(Integer.valueOf(i), graph.vertices(i).next().value("value"));
        graph.close();
    }

    @Test
    public void shouldPersistModificationsWithLastWriteWinning() {
        TinkerStorageGraph graph = open();
        final Vertex v = graph.addVertex(T.id, 1, "name", "original");
        graph.tx().commit();
        v.property("name", "updated");
        graph.tx().commit();
        graph.close();

        graph = open();
        assertEquals("updated", graph.vertices(1).next().value("name"));
        graph.close();
    }

    @Test
    public void shouldNotPersistRemovedElements() {
        TinkerStorageGraph graph = open();
        final Vertex a = graph.addVertex(T.id, 1);
        final Vertex b = graph.addVertex(T.id, 2);
        final Edge e = a.addEdge("knows", b, T.id, 10);
        graph.tx().commit();
        e.remove();
        b.remove();
        graph.tx().commit();
        graph.close();

        graph = open();
        assertEquals(1, countOf(graph.vertices()));
        assertEquals(0, countOf(graph.edges()));
        assertNotNull(graph.vertices(1).next());
        assertFalse(graph.vertices(2).hasNext());
        graph.close();
    }

    @Test
    public void shouldNotPersistRolledBackTransaction() {
        TinkerStorageGraph graph = open();
        graph.addVertex(T.id, 1, "name", "committed");
        graph.tx().commit();
        graph.addVertex(T.id, 2, "name", "rolledback");
        graph.tx().rollback();
        graph.close();

        graph = open();
        assertEquals(1, countOf(graph.vertices()));
        assertNotNull(graph.vertices(1).next());
        assertFalse(graph.vertices(2).hasNext());
        graph.close();
    }

    @Test
    public void shouldPersistMetaPropertiesAndMultiProperties() {
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        final Vertex v = graph.addVertex(T.id, 1);
        final VertexProperty<String> vp = v.property(VertexProperty.Cardinality.list, "name", "marko");
        vp.property("acl", "public");
        v.property(VertexProperty.Cardinality.list, "name", "marko a. rodriguez");
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(conf);
        final Vertex reV = graph.vertices(1).next();
        assertEquals(2, countOf(reV.properties("name")));
        final Iterator<VertexProperty<Object>> props = reV.properties("name");
        boolean foundAcl = false;
        while (props.hasNext()) {
            final VertexProperty<Object> p = props.next();
            if (p.properties("acl").hasNext()) {
                assertEquals("public", p.properties("acl").next().value());
                foundAcl = true;
            }
        }
        assertTrue("meta-property should survive persistence", foundAcl);
        graph.close();
    }

    @Test
    public void shouldPreserveStateAfterCompact() {
        TinkerStorageGraph graph = open();
        for (int i = 0; i < 5; i++) {
            graph.addVertex(T.id, i, "value", i);
            graph.tx().commit();
        }
        graph.compact();
        // keep writing after compaction to exercise the truncated log
        graph.addVertex(T.id, 100, "value", 100);
        graph.tx().commit();
        graph.close();

        graph = open();
        assertEquals(6, countOf(graph.vertices()));
        assertEquals(Integer.valueOf(100), graph.vertices(100).next().value("value"));
        assertEquals(Integer.valueOf(3), graph.vertices(3).next().value("value"));
        graph.close();
    }

    @Test
    public void shouldReopenEmptyGraph() {
        TinkerStorageGraph graph = open();
        graph.close();

        graph = open();
        assertEquals(0, countOf(graph.vertices()));
        assertEquals(0, countOf(graph.edges()));
        graph.close();
    }

    @Test
    public void shouldReportPersistenceFeature() {
        final TinkerStorageGraph graph = open();
        assertTrue(graph.features().graph().supportsPersistence());
        graph.close();
    }

    private static long countOf(final Iterator<?> it) {
        long count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }
}
