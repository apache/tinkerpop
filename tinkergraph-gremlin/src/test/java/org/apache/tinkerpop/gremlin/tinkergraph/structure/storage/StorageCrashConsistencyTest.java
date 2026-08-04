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
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, location);
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
        // WAL guarantee: a commit whose frame was durably written to the log, with no compaction having run, must
        // recover on reopen even though no snapshot exists.
        captureBuildingBlocks();
        Files.write(logFile.toPath(), logV2);
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

    private static long countOf(final Iterator<?> it) {
        long count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }
}
