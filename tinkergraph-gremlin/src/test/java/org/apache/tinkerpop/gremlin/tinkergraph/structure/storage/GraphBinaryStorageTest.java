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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerStorageGraph;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs the shared {@link AbstractTinkerStorageConformanceTest} suite against the {@link GraphBinaryStorage} engine and
 * adds engine-specific tests for the on-disk log layout.
 */
public class GraphBinaryStorageTest extends AbstractTinkerStorageConformanceTest {

    @Override
    protected String storageEngine() {
        return "graphbinary";
    }

    @Test
    public void shouldWriteSnapshotAndLogFiles() {
        final TinkerStorageGraph graph = open();
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        final String location = graph.configuration().getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION);
        assertTrue(new File(location, GraphBinaryStorage.LOG_FILE).exists());
        graph.close();
        // close compacts, producing a snapshot and truncating the log
        assertTrue(new File(location, GraphBinaryStorage.SNAPSHOT_FILE).exists());
    }

    @Test
    public void shouldPersistWithOsSyncMode() {
        // 'os' is a weaker durability mode (no fsync); a graceful close/reopen must still round-trip the data. The
        // OS-crash-loss window that distinguishes it from 'commit' cannot be exercised in a unit test.
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_SYNC, "os");
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        graph.addVertex(T.id, 1, "value", 42);
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(conf);
        assertEquals(1, countOf(graph.vertices()));
        assertEquals(Integer.valueOf(42), graph.vertices(1).next().value("value"));
        graph.close();
    }

    @Test
    public void shouldPersistWithDefaultCommitSyncMode() {
        // with no sync mode configured the engine defaults to 'commit' (fsync per commit); data must round-trip.
        TinkerStorageGraph graph = open();
        graph.addVertex(T.id, 1, "value", 42);
        graph.tx().commit();
        graph.close();

        graph = open();
        assertEquals(Integer.valueOf(42), graph.vertices(1).next().value("value"));
        graph.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUnknownSyncMode() {
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_SYNC, "bogus");
        TinkerStorageGraph.open(conf);
    }

    @Test
    public void shouldLeaveNoTempSnapshotAfterCompaction() {
        final TinkerStorageGraph graph = open();
        final String location = graph.configuration().getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION);
        graph.addVertex(T.id, 1, "value", 1);
        graph.tx().commit();
        graph.compact();
        // the atomic rename must consume the temp file, leaving a durable snapshot and no leftover .tmp
        assertTrue(new File(location, GraphBinaryStorage.SNAPSHOT_FILE).exists());
        assertTrue(!new File(location, GraphBinaryStorage.SNAPSHOT_FILE + ".tmp").exists());
        graph.close();
    }

    @Test
    public void shouldRecoverFromTruncatedTrailingFrame() throws Exception {
        TinkerStorageGraph graph = open();
        final String location = graph.configuration().getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION);
        graph.addVertex(T.id, 1, "value", 1);
        graph.tx().commit();
        graph.addVertex(T.id, 2, "value", 2);
        graph.tx().commit();
        graph.tx().close();

        // capture the raw log (two good frames) while the graph holds it, then close to release the directory lock.
        // close() compacts, folding the log into a snapshot, so we reconstruct a "crashed" on-disk state below.
        final File logFile = new File(location, GraphBinaryStorage.LOG_FILE);
        final byte[] goodLog = Files.readAllBytes(logFile.toPath());
        graph.close();

        // recreate the pre-crash layout: no snapshot, a log of the two good frames plus a torn trailing frame (a
        // length prefix promising 100 bytes with only a couple following), as an interrupted append would leave.
        Files.deleteIfExists(new File(location, GraphBinaryStorage.SNAPSHOT_FILE).toPath());
        try (final RandomAccessFile raf = new RandomAccessFile(logFile, "rw")) {
            raf.setLength(0);
            raf.write(goodLog);
            raf.writeInt(100);
            raf.write(new byte[]{ 0x01, 0x02 });
        }

        // reopening must recover the two fully-committed vertices and ignore the torn frame
        graph = open();
        assertEquals(2, countOf(graph.vertices()));
        assertEquals(Integer.valueOf(1), graph.vertices(1).next().value("value"));
        assertEquals(Integer.valueOf(2), graph.vertices(2).next().value("value"));
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
