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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerStorageGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * How a {@link TinkerStorageGraph} behaves when its storage engine fails while the process keeps running, using
 * {@link FaultInjectingStorage} to make the log or compaction fail on demand.
 */
public class StorageFailureTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        FaultInjectingStorage.reset();
        conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinkerStorageGraph.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, FaultInjectingStorage.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY,
                tempFolder.newFolder("storage").getAbsolutePath());
    }

    @After
    public void tearDown() {
        FaultInjectingStorage.reset();
    }

    @Test
    public void shouldRollBackMemoryAndIndexWhenLogFlushFails() {
        final TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        graph.createIndex("name", Vertex.class);
        graph.addVertex(T.id, 1, "name", "a", "value", 1);
        graph.tx().commit();

        FaultInjectingStorage.failLogWrites = true;
        graph.vertices(1).next().property("value", 2);
        graph.addVertex(T.id, 2, "name", "b");
        assertCommitFails(graph);
        FaultInjectingStorage.failLogWrites = false;

        // the same thread must not keep seeing the failed transaction's changes, and the index must not either
        assertEquals(Integer.valueOf(1), graph.vertices(1).next().value("value"));
        assertEquals(1, IteratorUtils.count(graph.vertices()));
        assertEquals(0L, (long) graph.traversal().V().has("name", "b").count().next());
        graph.tx().rollback();
        graph.close();
    }

    @Test
    public void shouldRefuseCommitsAfterLogFlushFailsAndRecoverOnReopen() {
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        graph.addVertex(T.id, 1);
        graph.tx().commit();

        FaultInjectingStorage.failLogWrites = true;
        graph.addVertex(T.id, 2);
        assertCommitFails(graph);
        FaultInjectingStorage.failLogWrites = false;

        // the disk works again, but the engine must not carry on: a later flush would also write the failed
        // transaction's buffered frame and make it durable
        graph.addVertex(T.id, 3);
        assertCommitFails(graph);
        graph.close();

        graph = TinkerStorageGraph.open(conf);
        assertEquals(Arrays.asList(1), vertexIds(graph));
        graph.addVertex(T.id, 4);
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(conf);
        assertEquals(Arrays.asList(1, 4), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldRefuseCommitsAfterLogAppendFails() {
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        graph.addVertex(T.id, 1);
        graph.tx().commit();

        // a frame larger than the log's buffer reaches the file during the append itself, before any flush
        FaultInjectingStorage.failLogWrites = true;
        final char[] large = new char[64 * 1024];
        Arrays.fill(large, 'x');
        graph.addVertex(T.id, 2, "value", new String(large));
        assertCommitFails(graph);
        FaultInjectingStorage.failLogWrites = false;

        graph.addVertex(T.id, 3);
        assertCommitFails(graph);
        graph.close();

        graph = TinkerStorageGraph.open(conf);
        assertEquals(Arrays.asList(1), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldKeepCommitWhenCompactionAfterItFails() {
        // compact after every commit so the failure lands right after the transaction is applied
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, 1);
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);

        FaultInjectingStorage.failCompaction = true;
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        FaultInjectingStorage.failCompaction = false;
        assertEquals(Arrays.asList(1), vertexIds(graph));

        graph.addVertex(T.id, 2);
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(conf);
        assertEquals(Arrays.asList(1, 2), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldKeepAcceptingCommitsWhenATransactionCannotBeEncoded() {
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        graph.addVertex(T.id, 1);
        graph.tx().commit();

        // a value with no serializer fails before anything is written, so the log is still sound
        graph.addVertex(T.id, 2, "value", new Object());
        assertCommitFails(graph);
        assertEquals(Arrays.asList(1), vertexIds(graph));

        graph.addVertex(T.id, 3);
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(conf);
        assertEquals(Arrays.asList(1, 3), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldCloseStorageWhenCompactionFailsOnClose() {
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        graph.addVertex(T.id, 1);
        graph.tx().commit();

        // the log still holds every acknowledged commit, so a compaction failure must not keep the graph from closing
        FaultInjectingStorage.failCompaction = true;
        graph.close();
        FaultInjectingStorage.failCompaction = false;

        // the closed graph must not write to a directory whose lock it has released
        graph.addVertex(T.id, 2);
        assertCommitFails(graph);

        final TinkerStorageGraph reopened = TinkerStorageGraph.open(conf);
        assertEquals(Arrays.asList(1), vertexIds(reopened));
        reopened.close();
    }

    @Test
    public void shouldNotRetryAFailedCompactionOnEveryCommit() {
        // a threshold well above one commit's frame, so that backing off by a threshold spans several commits
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, 512);
        final TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        FaultInjectingStorage.failCompaction = true;
        int id = 0;
        while (FaultInjectingStorage.compactionAttempts.get() == 0 && id < 1000) {
            graph.addVertex(T.id, ++id);
            graph.tx().commit();
        }
        assertEquals(1, FaultInjectingStorage.compactionAttempts.get());

        // the log is still over the threshold, but each compaction rewrites the whole graph under the commit lock,
        // so after a failure the next attempt waits for another threshold's worth of log
        for (int i = 0; i < 3; i++) {
            graph.addVertex(T.id, ++id);
            graph.tx().commit();
        }
        assertEquals(1, FaultInjectingStorage.compactionAttempts.get());

        FaultInjectingStorage.failCompaction = false;
        while (FaultInjectingStorage.compactionAttempts.get() == 1 && id < 2000) {
            graph.addVertex(T.id, ++id);
            graph.tx().commit();
        }
        assertEquals(2, FaultInjectingStorage.compactionAttempts.get());
        assertTrue(new File(conf.getString(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY),
                AbstractLogStorage.SNAPSHOT_FILE).exists());
        graph.close();
    }

    @Test
    public void shouldRemoveThePartialSnapshotWhenCompactionFails() {
        final TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        graph.addVertex(T.id, 1);
        graph.tx().commit();

        FaultInjectingStorage.failSnapshotWrites = true;
        try {
            graph.compact();
            fail("compaction should have failed");
        } catch (RuntimeException expected) {
            // expected
        }
        FaultInjectingStorage.failSnapshotWrites = false;

        final File directory = new File(conf.getString(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY));
        assertFalse("a failed compaction must not leave its partial snapshot behind",
                new File(directory, AbstractLogStorage.SNAPSHOT_FILE + ".tmp").exists());
        graph.close();
    }

    private static void assertCommitFails(final TinkerStorageGraph graph) {
        try {
            graph.tx().commit();
            fail("commit should have failed");
        } catch (RuntimeException expected) {
            // expected
        }
    }

    private static List<Integer> vertexIds(final TinkerStorageGraph graph) {
        final List<Integer> ids = new ArrayList<>();
        graph.vertices().forEachRemaining(v -> ids.add(((Number) v.id()).intValue()));
        Collections.sort(ids);
        return ids;
    }
}
