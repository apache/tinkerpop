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
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerStorageGraph;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Opening a damaged {@code graphbinary} store with {@code gremlin.tinkergraph.storage.recover}: what is recovered,
 * what is left out, and that the store on disk is never changed.
 */
public class StorageRecoveryTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String location;

    @Before
    public void setUp() throws Exception {
        location = tempFolder.newFolder("storage").getAbsolutePath();
    }

    @Test
    public void shouldRecoverTheLogUpToAFrameWithABadChecksum() throws Exception {
        final byte[] log = crashedLogOfThreeCommits();
        final List<int[]> frames = logFrames(log);
        log[frames.get(1)[2]] ^= 0x01; // flip a payload byte of the second commit
        writeStore(null, log);

        assertNormalOpenFails();
        final Map<String, String> before = directoryContents();

        final TinkerStorageGraph graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals(Collections.singletonList(1), vertexIds(graph));
        assertReadOnly(graph);
        graph.createIndex("name", Vertex.class);
        graph.close();

        assertEquals("recovery must not change the store", before, directoryContents());
        assertNormalOpenFails();
    }

    @Test
    public void shouldNotTruncateTheLogAtACorruptedFrameLength() throws Exception {
        final byte[] log = crashedLogOfThreeCommits();
        // a flipped high bit makes the second commit's length claim far more bytes than remain, which without a
        // checksum on the length would read as an interrupted append and cut the second and third commits off
        log[logFrames(log).get(1)[0]] ^= 0x40;
        writeStore(null, log);

        final Map<String, String> before = directoryContents();
        assertNormalOpenFails();
        assertEquals("a failed open must not change the store", before, directoryContents());

        final TinkerStorageGraph graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals(Collections.singletonList(1), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldFailWhenTheLogIsMissing() throws Exception {
        crashAfterCommitOnCompactedStore();
        Files.delete(logFile().toPath());

        assertNormalOpenFails();
        final TinkerStorageGraph graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals("recovery reads the snapshot", Collections.singletonList(1), vertexIds(graph));
        graph.close();
        assertTrue("recovery must not recreate the log", !logFile().exists());
    }

    @Test
    public void shouldFailWhenTheSnapshotIsMissing() throws Exception {
        crashAfterCommitOnCompactedStore();
        Files.delete(snapshotFile().toPath());

        // the log's records refer to the snapshot's dictionary, so not even recovery can read them
        for (final Configuration conf : Arrays.asList(config(), recoverConfig())) {
            try {
                TinkerStorageGraph.open(conf).close();
                fail("a log whose snapshot is gone should not open");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("snapshot"));
            }
        }
    }

    @Test
    public void shouldFailWhenTheVersionMarkerIsMissing() throws Exception {
        crashAfterCommitOnCompactedStore();
        Files.delete(new File(location, AbstractLogStorage.VERSION_FILE).toPath());

        assertNormalOpenFails();
        final TinkerStorageGraph graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals(Arrays.asList(1, 2), vertexIds(graph));
        graph.close();
        assertTrue("recovery must not write the marker", !new File(location, AbstractLogStorage.VERSION_FILE).exists());
    }

    @Test
    public void shouldFailWhenTheLogBelongsToAnotherSnapshot() throws Exception {
        crashAfterCommitOnCompactedStore();
        final byte[] log = Files.readAllBytes(logFile().toPath());
        ByteBuffer.wrap(log).putLong(AbstractLogStorage.MAGIC.length, 7);
        Files.write(logFile().toPath(), log);

        assertNormalOpenFails();
        final TinkerStorageGraph graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals("recovery reads only the snapshot", Collections.singletonList(1), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldCompleteACompactionInterruptedBeforeTheLogWasReplaced() throws Exception {
        crashAfterCommitOnCompactedStore();
        final byte[] oldLog = Files.readAllBytes(logFile().toPath());

        // close() compacts, moving the snapshot a generation ahead. Putting back the log it replaced leaves the store as
        // a crash between the snapshot's rename and the log's would.
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.close();
        Files.write(logFile().toPath(), oldLog);

        graph = TinkerStorageGraph.open(config());
        assertEquals(Arrays.asList(1, 2), vertexIds(graph));
        assertEquals("the old log is replaced by an empty one", AbstractLogStorage.HEADER_SIZE, logFile().length());
        graph.addVertex(T.id, 3);
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(config());
        assertEquals(Arrays.asList(1, 2, 3), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldRecoverTheLogUpToAFrameThatCannotBeDecoded() throws Exception {
        final byte[] log = crashedLogOfThreeCommits();
        // a well-formed frame with a valid checksum whose single entry has an unknown op code
        writeStore(null, replacePayload(log, logFrames(log).get(1), new byte[]{ 1, 99 }));

        assertNormalOpenFails();
        final TinkerStorageGraph graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals(Collections.singletonList(1), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldNotKeepPartOfATransactionThatFailsToDecode() throws Exception {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        graph.addVertex(T.id, 2);
        graph.addVertex(T.id, 3);
        graph.tx().commit();
        final byte[] log = Files.readAllBytes(logFile().toPath());
        graph.close();

        // drop the last byte of the second commit's payload, so its first vertex decodes and its second does not
        final int[] second = logFrames(log).get(1);
        final byte[] payload = Arrays.copyOfRange(log, second[2], second[2] + second[1] - 1);
        writeStore(null, replacePayload(log, second, payload));

        graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals("the damaged transaction must be left out entirely", Collections.singletonList(1), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldNotTruncateATornTailWhenRecovering() throws Exception {
        final byte[] log = crashedLogOfThreeCommits();
        final byte[] torn = Arrays.copyOf(log, log.length + 6);
        ByteBuffer.wrap(torn, log.length, 6).putInt(100); // a length prefix promising far more than follows
        writeStore(null, torn);

        final TinkerStorageGraph graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals(Arrays.asList(1, 2, 3), vertexIds(graph));
        graph.close();
        assertEquals("the torn tail is left in place", torn.length, logFile().length());
    }

    @Test
    public void shouldSkipAnUnreadableSnapshotElementAndDropEdgesThatLostAnEndpoint() throws Exception {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        final Vertex v1 = graph.addVertex(T.id, 1);
        final Vertex v2 = graph.addVertex(T.id, 2);
        final Vertex v3 = graph.addVertex(T.id, 3);
        v1.addEdge("knows", v2, T.id, 10);
        v2.addEdge("knows", v3, T.id, 11);
        v1.addEdge("knows", v3, T.id, 12);
        graph.tx().commit();
        graph.close(); // compacts everything into the snapshot

        // frame 0 is the dictionary, frames 1 to 3 are the vertices (in no guaranteed order), then the edges
        final byte[] snapshot = Files.readAllBytes(snapshotFile().toPath());
        snapshot[snapshotFrames(snapshot).get(2)[2]] ^= 0x01;
        writeStore(snapshot, null);

        assertNormalOpenFails();
        graph = TinkerStorageGraph.open(recoverConfig());
        final List<Integer> recovered = vertexIds(graph);
        assertEquals(2, recovered.size());
        final List<Integer> expectedEdges = new ArrayList<>();
        if (recovered.contains(1) && recovered.contains(2)) expectedEdges.add(10);
        if (recovered.contains(2) && recovered.contains(3)) expectedEdges.add(11);
        if (recovered.contains(1) && recovered.contains(3)) expectedEdges.add(12);
        assertEquals("only edges whose endpoints both survived", expectedEdges, edgeIds(graph));
        assertEquals("no vertex is invented for a lost endpoint", 2, vertexIds(graph).size());
        graph.close();
    }

    @Test
    public void shouldFailRecoveryWhenTheSnapshotDictionaryIsDamaged() throws Exception {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1, "name", "marko");
        graph.tx().commit();
        graph.close();

        final byte[] snapshot = Files.readAllBytes(snapshotFile().toPath());
        snapshot[snapshotFrames(snapshot).get(0)[2]] ^= 0x01;
        writeStore(snapshot, null);

        try {
            TinkerStorageGraph.open(recoverConfig());
            fail("nothing can be decoded without the dictionary, so recovery should fail");
        } catch (Exception expected) {
            // expected
        }
    }

    @Test
    public void shouldFailOnTrailingBytesInTheSnapshotUnlessRecovering() throws Exception {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1);
        graph.addVertex(T.id, 2);
        graph.tx().commit();
        graph.close();

        // a snapshot is written whole, so bytes that don't form a complete record are damage, not an interrupted append
        final byte[] snapshot = Files.readAllBytes(snapshotFile().toPath());
        writeStore(Arrays.copyOf(snapshot, snapshot.length + 3), null);

        assertNormalOpenFails();
        graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals(Arrays.asList(1, 2), vertexIds(graph));
        graph.close();
    }

    @Test
    public void shouldExportARecoveredGraphWithIo() throws Exception {
        final byte[] log = crashedLogOfThreeCommits();
        log[logFrames(log).get(2)[2]] ^= 0x01;
        writeStore(null, log);

        final String export = new File(tempFolder.getRoot(), "export.json").getAbsolutePath();
        final TinkerStorageGraph recovered = TinkerStorageGraph.open(recoverConfig());
        recovered.traversal().io(export).with(IO.writer, IO.graphson).write().iterate();
        recovered.close();

        final Configuration fresh = config();
        fresh.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, tempFolder.newFolder("fresh").getAbsolutePath());
        TinkerStorageGraph graph = TinkerStorageGraph.open(fresh);
        graph.traversal().io(export).with(IO.reader, IO.graphson).read().iterate();
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(fresh);
        assertEquals(Arrays.asList(1, 2), vertexIds(graph));
        assertEquals("marko", graph.vertices(1).next().value("name"));
        graph.close();
    }

    @Test
    public void shouldOpenAnUndamagedStoreReadOnlyWhenRecovering() {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(recoverConfig());
        assertEquals(Collections.singletonList(1), vertexIds(graph));
        assertReadOnly(graph);
        graph.close();
    }

    /**
     * Three commits, one vertex each, left as a log with no snapshot, as a crash before any compaction would leave.
     */
    private byte[] crashedLogOfThreeCommits() throws IOException {
        final TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1, "name", "marko");
        graph.tx().commit();
        graph.addVertex(T.id, 2, "name", "vadas");
        graph.tx().commit();
        graph.addVertex(T.id, 3, "name", "lop");
        graph.tx().commit();
        final byte[] log = Files.readAllBytes(logFile().toPath());
        graph.close();
        assertEquals(3, logFrames(log).size());
        return log;
    }

    /**
     * Leave the store as a crash would after one compaction and one more commit: a snapshot holding vertex 1 and a log
     * of the same generation holding vertex 2.
     */
    private void crashAfterCommitOnCompactedStore() throws IOException {
        TinkerStorageGraph graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 1);
        graph.tx().commit();
        graph.close();

        graph = TinkerStorageGraph.open(config());
        graph.addVertex(T.id, 2);
        graph.tx().commit();
        final byte[] snapshot = Files.readAllBytes(snapshotFile().toPath());
        final byte[] log = Files.readAllBytes(logFile().toPath());
        graph.close();
        writeStore(snapshot, log);
    }

    private void writeStore(final byte[] snapshot, final byte[] log) throws IOException {
        Files.deleteIfExists(snapshotFile().toPath());
        Files.deleteIfExists(logFile().toPath());
        if (snapshot != null)
            Files.write(snapshotFile().toPath(), snapshot);
        if (log != null)
            Files.write(logFile().toPath(), log);
    }

    private void assertNormalOpenFails() {
        try {
            TinkerStorageGraph.open(config()).close();
            fail("a normal open of the damaged store should fail");
        } catch (Exception expected) {
            // expected
        }
    }

    private static void assertReadOnly(final TinkerStorageGraph graph) {
        graph.addVertex(T.id, 99);
        try {
            graph.tx().commit();
            fail("a recovered graph should refuse commits");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
        }
        assertTrue("the refused commit must not be visible", !graph.vertices(99).hasNext());
        graph.tx().rollback();
    }

    private static List<int[]> logFrames(final byte[] log) {
        return frames(log, AbstractLogStorage.LOG_FRAME_HEADER_SIZE);
    }

    private static List<int[]> snapshotFrames(final byte[] snapshot) {
        return frames(snapshot, AbstractLogStorage.FRAME_HEADER_SIZE);
    }

    /**
     * The start offset, payload length and payload offset of each complete frame after the file header.
     */
    private static List<int[]> frames(final byte[] file, final int frameHeaderSize) {
        final List<int[]> frames = new ArrayList<>();
        final ByteBuffer buf = ByteBuffer.wrap(file);
        int offset = AbstractLogStorage.HEADER_SIZE;
        while (offset + frameHeaderSize <= file.length) {
            final int length = buf.getInt(offset);
            if (length < 0 || offset + frameHeaderSize + length > file.length)
                break;
            frames.add(new int[]{ offset, length, offset + frameHeaderSize });
            offset += frameHeaderSize + length;
        }
        return frames;
    }

    /**
     * Rebuild {@code log} with {@code frame}'s payload replaced, giving it a matching length, length checksum and
     * payload checksum.
     */
    private static byte[] replacePayload(final byte[] log, final int[] frame, final byte[] payload) {
        final int frameEnd = frame[2] + frame[1];
        final CRC32 lengthCrc = new CRC32();
        lengthCrc.update(ByteBuffer.allocate(Integer.BYTES).putInt(payload.length).array());
        final CRC32 crc = new CRC32();
        crc.update(payload);
        final ByteBuffer out = ByteBuffer.allocate(log.length - frame[1] + payload.length);
        out.put(log, 0, frame[0]);
        out.putInt(payload.length);
        out.putInt((int) lengthCrc.getValue());
        out.putInt((int) crc.getValue());
        out.put(payload);
        out.put(log, frameEnd, log.length - frameEnd);
        return out.array();
    }

    private Map<String, String> directoryContents() throws IOException {
        final Map<String, String> contents = new TreeMap<>();
        final File[] files = new File(location).listFiles();
        if (files != null) {
            for (final File f : files) {
                if (f.getName().equals(DirectoryLock.LOCK_FILE)) continue;
                final byte[] bytes = Files.readAllBytes(f.toPath());
                contents.put(f.getName(), bytes.length + ":" + Arrays.hashCode(bytes));
            }
        }
        return contents;
    }

    private Configuration config() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinkerStorageGraph.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, "graphbinary");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, location);
        return conf;
    }

    private Configuration recoverConfig() {
        final Configuration conf = config();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, true);
        return conf;
    }

    private File logFile() {
        return new File(location, AbstractLogStorage.LOG_FILE);
    }

    private File snapshotFile() {
        return new File(location, AbstractLogStorage.SNAPSHOT_FILE);
    }

    private static List<Integer> vertexIds(final TinkerStorageGraph graph) {
        final List<Integer> ids = new ArrayList<>();
        graph.vertices().forEachRemaining(v -> ids.add(((Number) v.id()).intValue()));
        Collections.sort(ids);
        return ids;
    }

    private static List<Integer> edgeIds(final TinkerStorageGraph graph) {
        final List<Integer> ids = new ArrayList<>();
        graph.edges().forEachRemaining((Edge e) -> ids.add(((Number) e.id()).intValue()));
        Collections.sort(ids);
        return ids;
    }
}
