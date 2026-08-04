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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerStorageGraph;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    public void shouldStreamSnapshotAsOneFramePerElement() throws Exception {
        final TinkerStorageGraph graph = open();
        final String location = graph.configuration().getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION);
        final Vertex a = graph.addVertex(T.id, 1, "name", "a");
        final Vertex b = graph.addVertex(T.id, 2, "name", "b");
        final Vertex c = graph.addVertex(T.id, 3, "name", "c");
        a.addEdge("knows", b, T.id, 10);
        b.addEdge("knows", c, T.id, 11);
        graph.tx().commit();
        graph.compact();

        // the snapshot must be written as one framed record per element (3 vertices + 2 edges = 5), rather than a
        // single whole-graph frame, so compaction never buffers the entire graph in one array
        final File snapshotFile = new File(location, GraphBinaryStorage.SNAPSHOT_FILE);
        assertEquals(5, countFrames(snapshotFile));
        graph.close();

        // and the streamed snapshot must reopen to exactly the same graph
        final TinkerStorageGraph reopened = open();
        assertEquals(3, countOf(reopened.vertices()));
        assertEquals(2, countOf(reopened.edges()));
        assertEquals("a", reopened.vertices(1).next().value("name"));
        assertEquals("knows", reopened.edges(10).next().label());
        reopened.close();
    }

    /**
     * Count the framed records in a storage file: a fixed header ({@code HEADER_SIZE} bytes) followed by frames of a
     * 4-byte big-endian payload length, a 4-byte CRC, then that many payload bytes.
     */
    private static int countFrames(final File file) throws Exception {
        int frames = 0;
        try (final DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(new java.io.FileInputStream(file)))) {
            final long headerSkipped = in.skip(GraphBinaryStorage.HEADER_SIZE);
            if (headerSkipped < GraphBinaryStorage.HEADER_SIZE) return 0;
            while (true) {
                final int len;
                try {
                    len = in.readInt();
                } catch (java.io.EOFException eof) {
                    break;
                }
                in.readInt(); // CRC
                final long skipped = in.skip(len);
                if (skipped < len) break;
                frames++;
            }
        }
        return frames;
    }

    @Test
    public void shouldAutoCompactWhenLogExceedsThreshold() {
        // a small threshold makes automatic compaction fire mid-run, without any explicit compact()/close()
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, 2048L);
        final String location = conf.getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION);
        TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        try {
            for (int i = 0; i < 200; i++) {
                graph.addVertex(T.id, i, "value", i, "pad", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
                graph.tx().commit();
            }
            // auto-compaction should have folded the log into a snapshot and truncated it well below the total
            // bytes written, so the live log stays bounded rather than growing with every commit
            final File logFile = new File(location, GraphBinaryStorage.LOG_FILE);
            final File snapshotFile = new File(location, GraphBinaryStorage.SNAPSHOT_FILE);
            assertTrue("expected a snapshot from auto-compaction", snapshotFile.exists());
            assertTrue("expected the live log to stay bounded, was " + logFile.length(),
                    logFile.length() < 8192);
        } finally {
            graph.close();
        }

        // data must survive across reopen despite the mid-run compactions
        graph = TinkerStorageGraph.open(conf);
        try {
            assertEquals(200, countOf(graph.vertices()));
            assertEquals(Integer.valueOf(199), graph.vertices(199).next().value("value"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldNotAutoCompactWhenThresholdIsZero() {
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, 0L);
        final String location = conf.getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION);
        final TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        try {
            for (int i = 0; i < 50; i++) {
                graph.addVertex(T.id, i, "pad", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
                graph.tx().commit();
            }
            // with auto-compaction disabled, no snapshot appears until close()/compact()
            assertTrue(!new File(location, GraphBinaryStorage.SNAPSHOT_FILE).exists());
        } finally {
            graph.close();
        }
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

    @Test
    public void shouldFailOnCorruptFrameWithBadCrc() throws Exception {
        TinkerStorageGraph graph = open();
        final String location = graph.configuration().getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION);
        graph.addVertex(T.id, 1, "value", 1);
        graph.tx().commit();
        graph.addVertex(T.id, 2, "value", 2);
        graph.tx().commit();
        graph.tx().close();

        // preserve the raw log, then reconstruct it with a bit flipped inside the first frame's payload — a complete
        // frame whose CRC no longer matches (distinct from a short trailing frame, which is tolerated as truncation)
        final File logFile = new File(location, GraphBinaryStorage.LOG_FILE);
        final byte[] log = Files.readAllBytes(logFile.toPath());
        graph.close();
        // header, then first frame's 4-byte length + 4-byte CRC, then payload — flip the first payload byte
        final int firstPayloadByte = GraphBinaryStorage.HEADER_SIZE + 2 * Integer.BYTES;
        log[firstPayloadByte] ^= 0x01;
        Files.deleteIfExists(new File(location, GraphBinaryStorage.SNAPSHOT_FILE).toPath());
        Files.write(logFile.toPath(), log);

        try {
            open();
            fail("expected reopen to fail on a CRC mismatch");
        } catch (Exception expected) {
            assertTrue("cause should report corruption: " + rootMessage(expected),
                    rootMessage(expected).contains("CRC mismatch"));
        }
    }

    @Test
    public void shouldFailOnForeignFileWithBadMagic() throws Exception {
        TinkerStorageGraph graph = open();
        final String location = graph.configuration().getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION);
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        graph.tx().close();
        final File logFile = new File(location, GraphBinaryStorage.LOG_FILE);
        final byte[] log = Files.readAllBytes(logFile.toPath());
        graph.close();

        // corrupt the magic so the file no longer identifies as a TinkerGraph storage file
        log[0] ^= 0xFF;
        Files.deleteIfExists(new File(location, GraphBinaryStorage.SNAPSHOT_FILE).toPath());
        Files.write(logFile.toPath(), log);

        try {
            open();
            fail("expected reopen to fail on bad magic");
        } catch (Exception expected) {
            assertTrue("cause should report a bad storage file: " + rootMessage(expected),
                    rootMessage(expected).contains("not a TinkerGraph storage file"));
        }
    }

    private static String rootMessage(final Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur)
            cur = cur.getCause();
        return String.valueOf(cur.getMessage());
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
