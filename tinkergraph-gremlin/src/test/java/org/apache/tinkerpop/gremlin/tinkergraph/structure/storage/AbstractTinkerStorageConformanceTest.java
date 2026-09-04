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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
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
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, location);
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

    @Test
    public void shouldPersistConcurrentCommitsWithoutLossAcrossReopen() throws Exception {
        // End-to-end companion to StorageCommitSerializationTest: many threads commit disjoint vertices at once and,
        // on reopen, every record must survive. This exercises the real engine but cannot by itself *prove* the lock
        // works — log corruption from interleaving is scheduling-dependent — so the deterministic guarantee is
        // asserted separately by StorageCommitSerializationTest via a probe engine.
        final int threads = 8;
        final int commitsPerThread = 50;
        final TinkerStorageGraph writeGraph = open();
        try {
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                futures.add(pool.submit(() -> {
                    start.await(); // release all threads together to maximize contention on the commit path
                    for (int i = 0; i < commitsPerThread; i++) {
                        final int id = threadId * commitsPerThread + i;
                        writeGraph.addVertex(T.id, id, "value", id);
                        writeGraph.tx().commit();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (final Future<?> f : futures)
                f.get(60, TimeUnit.SECONDS);
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        } finally {
            writeGraph.close();
        }

        // reopen from disk: a corrupt (interleaved) log frame would throw or drop records here
        final TinkerStorageGraph reopened = open();
        try {
            final int expected = threads * commitsPerThread;
            assertEquals(expected, countOf(reopened.vertices()));
            for (int id = 0; id < expected; id++)
                assertEquals(Integer.valueOf(id), reopened.vertices(id).next().value("value"));
        } finally {
            reopened.close();
        }
    }

    @Test
    public void shouldRoundTripDiverseValueTypes() {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("int", 42);
        values.put("long", 42L);
        values.put("float", 1.5f);
        values.put("double", 2.5d);
        values.put("bool", true);
        values.put("byte", (byte) 7);
        values.put("short", (short) 9);
        values.put("char", 'x');
        values.put("string", "hello");
        values.put("uuid", new UUID(12L, 34L));
        values.put("bigint", new BigInteger("123456789012345678901234567890"));
        values.put("bigdec", new BigDecimal("3.14159265358979"));
        values.put("datetime", OffsetDateTime.parse("2020-01-02T03:04:05Z"));
        values.put("duration", Duration.ofSeconds(90));

        TinkerStorageGraph graph = open();
        try {
            final Vertex v = graph.addVertex(T.id, 1);
            values.forEach(v::property);
            graph.tx().commit();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            final Vertex v = graph.vertices(1).next();
            values.forEach((k, expected) -> assertEquals(k, expected, v.value(k)));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldRoundTripCollectionValuedProperties() {
        final List<Object> list = Arrays.asList(1, "two", 3.0d);
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("b", "two");
        final Set<Object> set = new LinkedHashSet<>(Arrays.asList("x", "y", "z"));

        TinkerStorageGraph graph = open();
        try {
            final Vertex v = graph.addVertex(T.id, 1);
            v.property("list", list);
            v.property("map", map);
            v.property("set", set);
            graph.tx().commit();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            final Vertex v = graph.vertices(1).next();
            assertEquals(list, v.value("list"));
            assertEquals(map, v.value("map"));
            assertEquals(set, v.value("set"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldRoundTripNullPropertyValue() {
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, true);
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        try {
            final Vertex v = graph.addVertex(T.id, 1);
            v.property("maybe", null);
            graph.tx().commit();
        } finally {
            graph.close();
        }
        graph = TinkerStorageGraph.open(conf);
        try {
            final VertexProperty<Object> vp = graph.vertices(1).next().<Object>properties("maybe").next();
            assertTrue(vp.isPresent());
            assertNull(vp.value());
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldRoundTripHeterogeneousSameKeyTypes() {
        TinkerStorageGraph graph = open();
        try {
            graph.addVertex(T.id, 1, "k", 42);   // Integer
            graph.addVertex(T.id, 2, "k", 42L);  // Long
            graph.tx().commit();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            assertEquals(Integer.valueOf(42), graph.vertices(1).next().value("k"));
            assertEquals(Long.valueOf(42L), graph.vertices(2).next().value("k"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldRoundTripUuidElementIds() {
        roundTripElementIds(new UUID(0L, 1L), new UUID(0L, 2L), new UUID(0L, 10L));
    }

    @Test
    public void shouldRoundTripStringElementIds() {
        roundTripElementIds("v-1", "v-2", "e-10");
    }

    // uses the default ANY id manager so any id type is accepted verbatim; the point is that the storage codec
    // round-trips non-Long element ids through its scalar id encoding
    private void roundTripElementIds(final Object outId, final Object inId, final Object edgeId) {
        TinkerStorageGraph graph = open();
        try {
            final Vertex a = graph.addVertex(T.id, outId, "name", "a");
            final Vertex b = graph.addVertex(T.id, inId, "name", "b");
            a.addEdge("knows", b, T.id, edgeId, "weight", 0.5d);
            graph.tx().commit();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            assertEquals("a", graph.vertices(outId).next().value("name"));
            assertEquals(outId, graph.vertices(outId).next().id());
            final Edge e = graph.edges(edgeId).next();
            assertEquals("knows", e.label());
            assertEquals(outId, e.outVertex().id());
            assertEquals(inId, e.inVertex().id());
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldRoundTripLargeSchemaAcrossVarintBoundary() {
        // >127 distinct keys and >127 values under one key push dictionary refs and counts past the single-byte
        // LEB128 range, exercising the multi-byte varint path that small graphs never reach
        final int n = 200;
        // list cardinality so the >127 values under "multi" survive reopen (reconstruction takes cardinality from
        // graph config, not the stored record)
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, "list");
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        try {
            final Vertex v = graph.addVertex(T.id, 1);
            for (int i = 0; i < n; i++)
                v.property("key" + i, i);
            for (int i = 0; i < n; i++)
                v.property(VertexProperty.Cardinality.list, "multi", i);
            graph.tx().commit();
            graph.compact(); // also exercises a dictionary header with >127 entries
        } finally {
            graph.close();
        }
        graph = TinkerStorageGraph.open(conf);
        try {
            final Vertex v = graph.vertices(1).next();
            for (int i = 0; i < n; i++)
                assertEquals("key" + i, Integer.valueOf(i), v.value("key" + i));
            assertEquals(n, countOf(v.properties("multi")));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldRoundTripUnicodeKeysAndValues() {
        TinkerStorageGraph graph = open();
        try {
            final Vertex v = graph.addVertex(T.id, 1);
            v.property("naïve", "café");
            v.property("日本語", "テスト");
            v.property("emoji", "party🎉");
            graph.tx().commit();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            final Vertex v = graph.vertices(1).next();
            assertEquals("café", v.value("naïve"));
            assertEquals("テスト", v.value("日本語"));
            assertEquals("party🎉", v.value("emoji"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldSurviveRepeatedCompactionCycles() {
        TinkerStorageGraph graph = open();
        try {
            graph.addVertex(T.id, 1, "a", 1);
            graph.tx().commit();
            graph.compact();
            graph.addVertex(T.id, 2, "b", 2);
            graph.tx().commit();
            graph.compact();
            graph.addVertex(T.id, 3, "c", 3);
            graph.tx().commit();
            graph.compact();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            assertEquals(3, countOf(graph.vertices()));
            assertEquals(Integer.valueOf(1), graph.vertices(1).next().value("a"));
            assertEquals(Integer.valueOf(2), graph.vertices(2).next().value("b"));
            assertEquals(Integer.valueOf(3), graph.vertices(3).next().value("c"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldSurviveMultipleOpenCloseSessions() {
        TinkerStorageGraph graph = open();
        try {
            graph.addVertex(T.id, 1, "n", "one");
            graph.tx().commit();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            graph.addVertex(T.id, 2, "n", "two");
            graph.tx().commit();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            graph.addVertex(T.id, 3, "n", "three");
            graph.tx().commit();
            graph.compact();
        } finally {
            graph.close();
        }
        graph = open();
        try {
            assertEquals(3, countOf(graph.vertices()));
            assertEquals("one", graph.vertices(1).next().value("n"));
            assertEquals("two", graph.vertices(2).next().value("n"));
            assertEquals("three", graph.vertices(3).next().value("n"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldRoundTripConcurrentCommitsWithDistinctKeys() throws Exception {
        // concurrent commits that each introduce a distinct property key stress dictionary growth under the
        // commit-write lock; on reopen every distinct key must resolve
        final int threads = 8;
        final int perThread = 25;
        final TinkerStorageGraph writeGraph = open();
        try {
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int threadId = t;
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        final int id = threadId * perThread + i;
                        writeGraph.addVertex(T.id, id, "k_" + threadId + "_" + i, id);
                        writeGraph.tx().commit();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (final Future<?> f : futures)
                f.get(60, TimeUnit.SECONDS);
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        } finally {
            writeGraph.close();
        }
        final TinkerStorageGraph reopened = open();
        try {
            final int expected = threads * perThread;
            assertEquals(expected, countOf(reopened.vertices()));
            for (int t = 0; t < threads; t++) {
                for (int i = 0; i < perThread; i++) {
                    final int id = threadId(t, i, perThread);
                    assertEquals(Integer.valueOf(id), reopened.vertices(id).next().value("k_" + t + "_" + i));
                }
            }
        } finally {
            reopened.close();
        }
    }

    private static int threadId(final int t, final int i, final int perThread) {
        return t * perThread + i;
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
