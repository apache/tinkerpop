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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Crash-consistency tests for {@link GraphBinaryStorage}. Rather than kill a JVM mid-operation — which is slow and
 * non-deterministic — these reconstruct the exact on-disk states a crash would leave at each step of the two durable
 * sequences (the write-ahead commit and compaction) and assert that reopening recovers the correct graph. The
 * invariant under test: at no step may a crash leave the store unable to recover the last committed state.
 */
public class StorageCrashConsistencyTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String location;
    private File snapshotFile;
    private File logFile;
    private File tmpSnapshotFile;

    @Before
    public void setUp() throws Exception {
        final File dir = tempFolder.newFolder("storage");
        location = dir.getAbsolutePath();
        snapshotFile = new File(dir, GraphBinaryStorage.SNAPSHOT_FILE);
        logFile = new File(dir, GraphBinaryStorage.LOG_FILE);
        tmpSnapshotFile = new File(dir, GraphBinaryStorage.SNAPSHOT_FILE + ".tmp");
    }

    private Configuration config() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(Graph.GRAPH, TinkerStorageGraph.class.getName());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, "graphbinary");
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, location);
        // disable auto-compaction so tests control exactly when compaction happens
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, 0L);
        return conf;
    }

    private TinkerStorageGraph open() {
        return TinkerStorageGraph.open(config());
    }

    /**
     * Captured building blocks of valid on-disk files, used to assemble crash states: a snapshot holding {1}, a log
     * holding a later commit of {2}, and a compacted snapshot holding {1,2}.
     */
    private byte[] snapshotV1;
    private byte[] logV2;
    private byte[] snapshotV12;

    private void captureBuildingBlocks() throws IOException {
        // snapshot holding vertex 1 (compaction on close folds the single commit into the snapshot)
        TinkerStorageGraph g = open();
        g.addVertex(T.id, 1, "value", 1);
        g.tx().commit();
        g.close();
        snapshotV1 = Files.readAllBytes(snapshotFile.toPath());

        // a valid log holding a later commit of vertex 2, captured before the closing compaction folds it away
        g = open(); // replays {1} from the snapshot
        g.addVertex(T.id, 2, "value", 2);
        g.tx().commit();
        g.tx().close();
        logV2 = Files.readAllBytes(logFile.toPath());
        g.close(); // compacts {1,2} into the snapshot
        snapshotV12 = Files.readAllBytes(snapshotFile.toPath());

        // reset the directory to a clean slate for the state under test
        resetFiles();
    }

    private void resetFiles() throws IOException {
        Files.deleteIfExists(snapshotFile.toPath());
        Files.deleteIfExists(logFile.toPath());
        Files.deleteIfExists(tmpSnapshotFile.toPath());
    }

    private void assertReopensTo(final int... expectedIds) {
        final TinkerStorageGraph graph = open();
        try {
            assertEquals(expectedIds.length, countOf(graph.vertices()));
            for (final int id : expectedIds)
                assertEquals(Integer.valueOf(id), graph.vertices(id).next().value("value"));
        } finally {
            graph.close();
        }
    }

    @Test
    public void shouldRecoverDurableCommitWithNoSnapshot() throws Exception {
        // WAL guarantee: a commit durably written to the log with no compaction (no snapshot) recovers on reopen. On
        // a fresh store the dictionary starts empty, so the commit frame is self-contained (it carries its own
        // dictionary appends), which is exactly the real "log only, never compacted" case.
        final TinkerStorageGraph g = open();
        g.addVertex(T.id, 2, "value", 2);
        g.tx().commit();
        g.tx().close();
        final byte[] selfContainedLog = Files.readAllBytes(logFile.toPath());
        g.close(); // compaction on close would fold the log away; the raw log was captured above
        resetFiles();
        Files.write(logFile.toPath(), selfContainedLog);
        assertReopensTo(2);
    }

    @Test
    public void shouldRecoverFromSnapshotPlusLog() throws Exception {
        // steady pre-compaction state: a snapshot holding earlier commits and a log holding later ones. Reopen must
        // fold snapshot-then-log into the union.
        captureBuildingBlocks();
        Files.write(snapshotFile.toPath(), snapshotV1);
        Files.write(logFile.toPath(), logV2);
        assertReopensTo(1, 2);
    }

    @Test
    public void shouldIgnoreStrayTempSnapshotFromCrashBeforeRename() throws Exception {
        // crash after writing snapshot.gbin.tmp but before the atomic rename: the old snapshot + log are intact and
        // the stray .tmp must be ignored, so the last committed state still recovers.
        captureBuildingBlocks();
        Files.write(snapshotFile.toPath(), snapshotV1);
        Files.write(logFile.toPath(), logV2);
        Files.write(tmpSnapshotFile.toPath(), new byte[]{ 0x00, 0x01, 0x02, 0x03 }); // garbage half-written temp
        assertReopensTo(1, 2);
    }

    @Test
    public void shouldRecoverFromNewSnapshotWithLogNotYetDeleted() throws Exception {
        // crash after the rename installed the new snapshot but before the log was truncated: snapshot holds {1,2}
        // and the stale log still holds {2}. Folding snapshot-then-log is idempotent (last write per id wins), so the
        // result is exactly {1,2} — never a lost or duplicated element.
        captureBuildingBlocks();
        Files.write(snapshotFile.toPath(), snapshotV12);
        Files.write(logFile.toPath(), logV2);
        assertReopensTo(1, 2);
    }

    @Test
    public void shouldRecoverCrashWindowWithADeadKeyForcingDictionaryDivergence() throws Exception {
        // Guards the preserve-dictionary-numbering decision. A key ("alpha") is deleted from the live graph after it
        // is in the dictionary, then a new key ("gamma") is added. Preserving numbering keeps alpha's id forever, so
        // the surviving log's gamma ref still matches the new snapshot's dictionary. Renumbering on compaction would
        // instead drop the now-dead alpha and shift gamma to a lower id, so the log's higher-numbered gamma ref would
        // no longer resolve. A single-key (or no-delete) state cannot tell the two apart.
        final TinkerStorageGraph g = open();
        g.addVertex(T.id, 1, "alpha", 1);
        g.tx().commit();
        g.addVertex(T.id, 2, "beta", 2);
        g.tx().commit();
        g.compact(); // snapshot holds alpha and beta in the dictionary at stable ids

        g.vertices(1).next().remove(); // alpha becomes a dead key: retained only under preserve-numbering
        g.tx().commit();
        // re-write the surviving vertex: its record now carries a bare reference to the pre-existing key "beta"
        // (not re-appended) plus a new key "gamma". If compaction renumbered, "beta"'s id would shift and this bare
        // reference would resolve to the wrong key on replay.
        g.vertices(2).next().property("gamma", "g");
        g.tx().commit();
        final byte[] logNotYetDeleted = Files.readAllBytes(logFile.toPath());
        g.compact(); // new full-dictionary snapshot (preserved numbering), then log truncated
        final byte[] newSnapshot = Files.readAllBytes(snapshotFile.toPath());
        g.close();

        // reconstruct the crash window: new snapshot in place, old log not yet deleted
        Files.write(snapshotFile.toPath(), newSnapshot);
        Files.write(logFile.toPath(), logNotYetDeleted);

        final TinkerStorageGraph reopened = open();
        try {
            assertEquals(1, countOf(reopened.vertices())); // only the surviving vertex 2
            assertEquals(0, countOf(reopened.vertices(1))); // alpha's vertex was deleted
            final Vertex v = reopened.vertices(2).next();
            assertEquals(Integer.valueOf(2), v.value("beta"));
            assertEquals("g", v.value("gamma"));
        } finally {
            reopened.close();
        }
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
