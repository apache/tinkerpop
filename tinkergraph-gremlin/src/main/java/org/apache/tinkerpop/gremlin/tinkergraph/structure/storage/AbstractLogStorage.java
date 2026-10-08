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
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * Log-structured durable storage machinery shared by {@link TinkerStorage} engines, independent of how an element is
 * encoded. It persists a {@code TinkerStorageGraph} as an append-only commit log ({@code log.gbin}) plus an optional
 * folded {@code snapshot.gbin}; on open the snapshot is read followed by the log, last-write-wins per element id.
 * <p/>
 * This base owns everything that is not the element codec: the on-disk file layout, the single-source-of-truth
 * {@code VERSION} marker, the compaction generation that ties a log to its snapshot, length+CRC frame framing (which
 * tells an interrupted trailing append apart from genuine corruption, with log frames also checksumming their length),
 * the replay fold loop, durability via {@link SyncMode}, and crash-safe atomic compaction with a size threshold. Concrete engines supply only the codec through {@link #encodeCommit}, {@link #decodeFrame},
 * {@link #writeSnapshot}, and (optionally) {@link #beginReplay} for per-replay decode state.
 * <p/>
 * The in-memory graph remains authoritative (write-through); this machinery does not support graphs larger than memory.
 */
public abstract class AbstractLogStorage implements TinkerStorage {

    private static final Logger logger = LoggerFactory.getLogger(AbstractLogStorage.class);

    /**
     * Magic bytes ("TGSB" — TinkerGraph Storage Binary) at the start of every storage file, so a file can be
     * identified as one written by this engine (and a foreign or corrupt file rejected) before any record is read.
     */
    static final byte[] MAGIC = { 'T', 'G', 'S', 'B' };

    /**
     * On-disk format version of the store. Recorded once per store in the {@link #VERSION_FILE} marker rather than in
     * every file, so a store has a single unambiguous version even when it momentarily holds a snapshot and a log
     * written at different times. A future format bump is detected against this marker so an older store is rejected
     * (never silently misread); the supported migration path is to export via {@code g.io()} before upgrading.
     */
    static final byte FORMAT_VERSION = 1;

    /**
     * Store-level version marker file, holding {@link #MAGIC} followed by the one-byte {@link #FORMAT_VERSION}.
     */
    static final String VERSION_FILE = "VERSION";

    /**
     * Bytes of the per-file header: {@link #MAGIC} followed by the file's 8-byte compaction generation. The format
     * version lives in the store-level {@link #VERSION_FILE}, not in each file.
     * <p/>
     * The generation is what lets an open tell a damaged store from a legitimate one. Each compaction writes a snapshot
     * of the next generation and then replaces the log with an empty one of that same generation, so a store with data
     * always has a log, the log's generation always names the snapshot it builds on (or {@code 0} when there is none),
     * and only the crash window between those two renames leaves the snapshot one generation ahead of the log. Any other
     * combination, such as a missing log or a log whose snapshot is gone, means files were lost.
     */
    static final int HEADER_SIZE = MAGIC.length + Long.BYTES;

    /**
     * Bytes ahead of each snapshot frame's payload: a 4-byte length and a 4-byte CRC32 of the payload.
     */
    static final int FRAME_HEADER_SIZE = 2 * Integer.BYTES;

    /**
     * Bytes ahead of each log frame's payload: a 4-byte length, a 4-byte CRC32 of that length, and a 4-byte CRC32 of
     * the payload. The log is the only file whose tail is ever cut back, and a length that claims more bytes than
     * remain is what marks a torn tail, so the length must be verifiable on its own. Without that, one corrupted length
     * in the middle of the log would read as a torn tail and every commit after it would be cut off. The snapshot is
     * only ever replaced whole, so its frames do without the extra check.
     */
    static final int LOG_FRAME_HEADER_SIZE = 3 * Integer.BYTES;

    static final String SNAPSHOT_FILE = "snapshot.gbin";
    static final String LOG_FILE = "log.gbin";

    /**
     * Default automatic-compaction threshold: 64 MB of appended log since the last compaction.
     */
    static final long DEFAULT_COMPACT_THRESHOLD_BYTES = 64L * 1024 * 1024;

    private File directory;
    private File snapshotFile;
    private File logFile;
    private File versionFile;

    private DataOutputStream logOut;
    private FileOutputStream logFos;
    private SyncMode syncMode = SyncMode.COMMIT;
    private long compactThresholdBytes = DEFAULT_COMPACT_THRESHOLD_BYTES;
    private long logBytesSinceCompaction = 0;

    /**
     * After an automatic compaction fails, the log size it must reach before the next attempt. Each attempt rewrites
     * the whole graph while every commit waits on the commit lock, so a compaction that keeps failing (a full disk, say)
     * is retried after another threshold's worth of log rather than on every commit. Reset by a compaction that
     * succeeds.
     */
    private long compactionDeferredUntil = 0;

    /**
     * The compaction generation of the current log, and of the snapshot it builds on when there is one.
     */
    private long generation = 0;

    /**
     * Whether replay should fold the log. Only a recovery open that found the log missing, or not belonging to the
     * snapshot, leaves it out.
     */
    private boolean replayLog = true;

    /**
     * What a recovery open had to leave behind, or {@code null} for a normal open. Created on open so the checks made
     * there can add to what replay later reports.
     */
    private Recovery recovery;

    private boolean closed = false;

    /**
     * The first failure to write or flush the log, or {@code null} while the log is sound. Once set, the engine
     * fail-stops: no further commit is accepted, nothing still buffered is written, and only a reopen (which replays
     * what actually reached disk) recovers. After a failed write or fsync, neither the buffered bytes nor the
     * operating system's view of the file can be trusted, so carrying on could make a failed transaction durable.
     * Accessed only under the graph's storage commit lock.
     */
    private IOException failure;

    /**
     * Whether the store was opened with {@code gremlin.tinkergraph.storage.recover}. Replay then recovers what it can
     * from a damaged snapshot or log instead of failing, and the engine stays read-only so that nothing on disk
     * changes: commits are refused, nothing is compacted or truncated, and no version marker is written.
     */
    private boolean recovering = false;

    // ----------------------------------------------------------------------------------------- codec hooks

    /**
     * Encode a committing transaction's changeset into a single record payload (the framing is added by the caller).
     * A failed encode fails only its own transaction and later commits are still accepted, so on failure an
     * implementation must undo any codec state it changed while encoding, such as dictionary entries.
     */
    protected abstract byte[] encodeCommit(long txVersion,
                                           Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                                           Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) throws IOException;

    /**
     * Decode one record payload, folding its puts and deletes into the supplied maps (last-write-wins per id).
     */
    protected abstract void decodeFrame(byte[] record,
                                        Map<Object, DetachedVertex> vertices,
                                        Map<Object, DetachedEdge> edges) throws IOException;

    /**
     * Write the entire current committed state of the graph to {@code out} as framed records (via {@link #writeFrame}),
     * for compaction. The file header has already been written to {@code out}.
     */
    protected abstract void writeSnapshot(AbstractTinkerGraph graph, DataOutputStream out) throws IOException;

    /**
     * Reset any per-replay decode state (e.g. a dictionary) before a fold begins. Default is a no-op.
     */
    protected void beginReplay() {
        // no-op by default
    }

    // ----------------------------------------------------------------------------------------- lifecycle

    @Override
    public void open(final AbstractTinkerGraph graph, final Configuration config) {
        final String location = config.getString(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, null);
        if (null == location)
            throw new IllegalStateException(String.format("%s must be set to use a durable storage engine",
                    TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY));
        this.directory = new File(location);
        this.snapshotFile = new File(directory, SNAPSHOT_FILE);
        this.logFile = new File(directory, LOG_FILE);
        this.versionFile = new File(directory, VERSION_FILE);
        this.recovering = config.getBoolean(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, false);
        this.syncMode = SyncMode.fromConfigValue(config.getString(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_SYNC, null));
        this.compactThresholdBytes = config.getLong(
                TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, DEFAULT_COMPACT_THRESHOLD_BYTES);
        // seed the counter with any pre-existing log so a graph reopened with a large log still compacts promptly
        this.logBytesSinceCompaction = logFile.exists() ? Math.max(0, logFile.length() - HEADER_SIZE) : 0;
        this.recovery = recovering ? new Recovery() : null;
        configureCodec(config);
        ensureDirectory();
        establishStoreVersion();
        establishGeneration();
    }

    /**
     * Read any codec-specific configuration. Called once during {@link #open}. Default is a no-op.
     */
    protected void configureCodec(final Configuration config) {
        // no-op by default
    }

    @Override
    public void replay(final AbstractTinkerGraph graph) {
        beginReplay();
        // Fold snapshot then log into final state: last write per id wins, deletes remove.
        final Map<Object, DetachedVertex> vertices = new LinkedHashMap<>();
        final Map<Object, DetachedEdge> edges = new LinkedHashMap<>();

        // the snapshot is only ever replaced by an atomic rename, so only the log can end in an interrupted append
        if (snapshotFile.exists())
            foldRecords(snapshotFile, vertices, edges, false, recovery);
        if (replayLog && logFile.exists()) {
            final long completeEnd = foldRecords(logFile, vertices, edges, true, recovery);
            // a recovery open never changes the files, so a torn tail is left in place
            if (recovery == null)
                truncateTornLogTail(completeEnd);
            else if (logFile.length() > completeEnd && recovery.logStoppedAt < 0)
                recovery.logStopped(completeEnd, logFile.length(), "the log ends in an interrupted append");
        }

        if (recovery != null) {
            // getOrCreate would invent a bare vertex for a missing endpoint, so drop edges that lost one instead
            final Iterator<DetachedEdge> it = edges.values().iterator();
            while (it.hasNext()) {
                final DetachedEdge e = it.next();
                if (!vertices.containsKey(e.outVertex().id()) || !vertices.containsKey(e.inVertex().id())) {
                    it.remove();
                    recovery.danglingEdges++;
                }
            }
            recovery.report(directory);
        }

        if (vertices.isEmpty() && edges.isEmpty())
            return;

        // Attach vertices first so edges can find their endpoints, then commit once.
        for (final DetachedVertex v : vertices.values())
            v.attach(Attachable.Method.getOrCreate(graph));
        for (final DetachedEdge e : edges.values())
            e.attach(Attachable.Method.getOrCreate(graph));

        graph.tx().commit();
    }

    @Override
    public void persist(final long txVersion,
                        final Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                        final Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) {
        checkNotClosed();
        checkNotRecovering();
        checkNotFailed();
        ensureLogOpen();
        final byte[] frame;
        try {
            frame = encodeCommit(txVersion, changedVertices, changedEdges);
        } catch (IOException ex) {
            // nothing has been written yet, so the log is still sound and only this transaction fails
            throw new UncheckedIOException("Could not encode transaction for storage log: " + ex.getMessage(), ex);
        }
        try {
            writeLogFrame(logOut, frame);
        } catch (IOException ex) {
            throw fail("Could not append transaction to storage log", ex);
        }
        logBytesSinceCompaction += LOG_FRAME_HEADER_SIZE + frame.length;
    }

    @Override
    public void flush() {
        if (closed || recovering || failure != null)
            return;
        if (logOut != null) {
            try {
                // flush the JVM buffer into the OS page cache; durable against a JVM process crash
                logOut.flush();
                // in COMMIT mode also force the OS page cache to the device, so an acknowledged commit is durable
                // against an OS crash or power loss. OS mode stops at the flush above and accepts that weaker guarantee.
                if (syncMode == SyncMode.COMMIT)
                    logFos.getFD().sync();
            } catch (IOException ex) {
                throw fail("Could not flush storage log", ex);
            }
        }
    }

    @Override
    public void compact(final AbstractTinkerGraph graph) {
        // after a write failure the disk is suspect and the log's buffered tail must not be written, so leave the
        // files as they are for the next open to replay
        if (closed || recovering || failure != null)
            return;
        // Write a fresh snapshot of the current committed state at the next generation, then replace the log with an
        // empty one of that generation. This must be crash-safe: at no point may a crash leave the store without a
        // readable snapshot-or-log covering the committed state. Ordering is write-tmp -> fsync tmp -> atomically
        // rename tmp over the snapshot -> fsync dir (the rename is now durable) -> install the empty log the same way.
        // Each file is only ever replaced by an atomic rename, so a crash at any step leaves either the old snapshot
        // and log, the new snapshot with the old log (which open recognizes by its generation), or the new pair.
        closeLog();
        ensureDirectory();
        final long nextGeneration = generation + 1;
        final File tmp = new File(directory, SNAPSHOT_FILE + ".tmp");
        try (final FileOutputStream fos = new FileOutputStream(tmp);
             final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fos))) {
            writeHeader(out, nextGeneration);
            writeSnapshot(graph, out);
            out.flush();
            // force the snapshot's bytes to the device before it is renamed into place
            fos.getFD().sync();
        } catch (IOException ex) {
            deleteQuietly(tmp);
            throw new UncheckedIOException("Could not write storage snapshot", ex);
        }

        try {
            // atomically replace the snapshot; no delete-then-rename window where the snapshot is briefly absent
            try {
                atomicMove(tmp, snapshotFile);
            } catch (IOException ex) {
                deleteQuietly(tmp);
                throw ex;
            }
            // fsync the directory so the rename survives a crash before we touch the log
            syncDirectory();
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not finalize storage snapshot", ex);
        }

        // empty the log now that the snapshot durably reflects the committed state. Once the snapshot is in place, the
        // old log may only stay as it is, since open replaces it unread as an interrupted compaction. Appending to it
        // would put commits there that the next open discards, so a failure here stops further commits instead.
        try {
            installEmptyLog(nextGeneration);
        } catch (IOException ex) {
            throw fail("Could not replace storage log after writing a new snapshot", ex);
        }
        generation = nextGeneration;

        // the log is now empty; the accumulated state lives in the snapshot
        logBytesSinceCompaction = 0;
        compactionDeferredUntil = 0;
    }

    @Override
    public void maybeCompact(final AbstractTinkerGraph graph) {
        if (closed || compactThresholdBytes <= 0)
            return;
        if (logBytesSinceCompaction < compactThresholdBytes || logBytesSinceCompaction < compactionDeferredUntil)
            return;
        try {
            compact(graph);
        } catch (RuntimeException ex) {
            compactionDeferredUntil = logBytesSinceCompaction + compactThresholdBytes;
            throw ex;
        }
    }

    @Override
    public boolean isReadOnly() {
        return recovering;
    }

    @Override
    public void close() {
        try {
            if (failure != null) {
                logger.warn("Closing storage at {} after an earlier write failure; anything not yet written is " +
                        "discarded and the next open recovers from what is on disk", directory);
                discardLog();
            } else {
                closeLog();
            }
        } finally {
            closed = true;
        }
    }

    // ----------------------------------------------------------------------------------------- version marker

    /**
     * Read and validate the store-level version marker, or create it for a new store. This is the single source of
     * truth for the store's format version: a marker naming an unsupported version, or bad magic, fails the open
     * loudly rather than risking a misread.
     */
    private void establishStoreVersion() {
        final boolean storeHasData = snapshotFile.exists() || logFile.exists();
        if (!versionFile.exists()) {
            if (storeHasData) {
                final String problem = String.format("Storage location %s has data but no %s marker, so its format " +
                        "version cannot be confirmed", directory, VERSION_FILE);
                if (!recovering)
                    throw new IllegalStateException(problem + String.format(
                            "; open it with %s to read it as format version %d",
                            TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, FORMAT_VERSION));
                recovery.note(problem + String.format("; it was read as format version %d.", FORMAT_VERSION));
            } else if (!recovering) {
                writeStoreVersion();
            }
            return;
        }
        try (final DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(versionFile)))) {
            final byte[] magic = new byte[MAGIC.length];
            readFully(in, magic);
            if (!Arrays.equals(magic, MAGIC))
                throw new IOException(String.format("%s is not a TinkerGraph storage version marker (bad magic)", versionFile));
            final byte version = in.readByte();
            if (version != FORMAT_VERSION)
                throw new IOException(String.format(
                        "Unsupported storage format version %d at %s (this build writes %d); open it with the TinkerPop " +
                                "version that wrote it and export it with g.io()",
                        version, directory, FORMAT_VERSION));
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage version marker %s", versionFile), ex);
        }
    }

    private void writeStoreVersion() {
        try (final FileOutputStream fos = new FileOutputStream(versionFile);
             final DataOutputStream out = new DataOutputStream(fos)) {
            out.write(MAGIC);
            out.writeByte(FORMAT_VERSION);
            out.flush();
            fos.getFD().sync();
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not write storage version marker %s", versionFile), ex);
        }
    }

    /**
     * Check that the snapshot and log belong together, using the generation in each header (see {@link #HEADER_SIZE}),
     * and settle the generation the engine continues from. A new store gets its empty log here. A snapshot one
     * generation ahead of the log is a compaction interrupted between its two renames: the snapshot already holds
     * everything the old log does, so the empty log that compaction would have installed is installed now. Any other
     * mismatch means files were lost and fails the open. A recovery open changes nothing, reports the problem, and
     * reads what remains, except that a log whose snapshot is gone still fails, since its records refer to the
     * snapshot's dictionary.
     */
    private void establishGeneration() {
        final boolean hasSnapshot = snapshotFile.exists();
        final boolean hasLog = logFile.exists();
        if (!hasSnapshot && !hasLog) {
            if (!recovering)
                installNewLog(0);
            return;
        }

        final long snapshotGeneration = hasSnapshot ? readGeneration(snapshotFile) : -1;
        if (!hasLog) {
            final String problem = String.format("Storage log %s is missing, so any transactions committed after the " +
                    "last compaction are lost", logFile);
            if (!recovering)
                throw new IllegalStateException(problem + String.format(
                        "; open the store with %s to read its snapshot", TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
            recovery.note(problem + "; only the snapshot was read.");
            replayLog = false;
            return;
        }

        final long logGeneration;
        try {
            logGeneration = readGeneration(logFile);
        } catch (UncheckedIOException ex) {
            // a recovery open lets the fold report an unreadable log header and carry on with the snapshot
            if (!recovering)
                throw ex;
            generation = Math.max(0, snapshotGeneration);
            return;
        }

        if (!hasSnapshot) {
            if (logGeneration != 0)
                throw new IllegalStateException(String.format("Storage snapshot %s is missing: log %s was written " +
                        "after compaction %d and its records depend on that snapshot", snapshotFile, logFile, logGeneration));
            generation = 0;
            return;
        }

        if (logGeneration == snapshotGeneration) {
            generation = snapshotGeneration;
        } else if (logGeneration == snapshotGeneration - 1) {
            generation = snapshotGeneration;
            if (!recovering) {
                logger.warn("Storage at {} was interrupted while compacting; completing it by emptying the log, " +
                        "whose transactions are all in the new snapshot", directory);
                installNewLog(snapshotGeneration);
                logBytesSinceCompaction = 0;
            }
        } else {
            final String problem = String.format("Storage log %s (generation %d) does not belong to snapshot %s " +
                    "(generation %d)", logFile, logGeneration, snapshotFile, snapshotGeneration);
            if (!recovering)
                throw new IllegalStateException(problem + String.format(
                        "; open the store with %s to read its snapshot", TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
            recovery.note(problem + "; only the snapshot was read.");
            generation = snapshotGeneration;
            replayLog = false;
        }
    }

    /**
     * Read the generation from the header of {@code file}, failing if the header is short or is not one of ours.
     */
    private static long readGeneration(final File file) {
        try (final DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            readAndVerifyHeader(in, file, file.length());
            return in.readLong();
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage file %s", file), ex);
        }
    }

    private void installNewLog(final long logGeneration) {
        try {
            installEmptyLog(logGeneration);
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not create storage log %s", logFile), ex);
        }
    }

    /**
     * Atomically replace the log with one holding only its header at {@code logGeneration}. The log is never deleted,
     * so its absence is always damage, and never written in place, so it is never shorter than its header.
     */
    private void installEmptyLog(final long logGeneration) throws IOException {
        final File tmp = new File(directory, LOG_FILE + ".tmp");
        try (final FileOutputStream fos = new FileOutputStream(tmp);
             final DataOutputStream out = new DataOutputStream(fos)) {
            writeHeader(out, logGeneration);
            out.flush();
            fos.getFD().sync();
        } catch (IOException ex) {
            deleteQuietly(tmp);
            throw ex;
        }
        try {
            atomicMove(tmp, logFile);
        } catch (IOException ex) {
            deleteQuietly(tmp);
            throw ex;
        }
        syncDirectory();
    }

    private void ensureDirectory() {
        if (directory.exists()) {
            if (!directory.isDirectory())
                throw new IllegalStateException(String.format("Storage location %s exists but is not a directory", directory));
        } else if (!directory.mkdirs()) {
            throw new IllegalStateException(String.format("Could not create storage directory %s", directory));
        }
    }

    // ----------------------------------------------------------------------------------------- fold / framing

    /**
     * Fold every complete frame of {@code file} into the given maps, returning the byte offset at which the last
     * complete frame ends. Anything past that offset is an interrupted trailing append.
     * <p/>
     * With a {@code recovery} in progress, damage that would otherwise fail the open is tolerated where that is safe.
     * In the log, each frame is one transaction, so replay stops at the first frame that can't be read or decoded and
     * keeps the consistent prefix before it. In the snapshot, element frames are independent, so an unreadable one is
     * skipped. The snapshot's header and its first frame, the dictionary every other frame depends on, still fail.
     */
    private long foldRecords(final File file, final Map<Object, DetachedVertex> vertices,
                             final Map<Object, DetachedEdge> edges, final boolean log, final Recovery recovery) {
        final long fileLength = file.length();
        try (final DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            long remaining;
            try {
                remaining = readAndVerifyHeader(in, file, fileLength);
                in.readLong(); // the generation, already checked on open
            } catch (IOException ex) {
                if (recovery == null || !log)
                    throw ex;
                recovery.logStopped(0, fileLength, ex.getMessage());
                return 0;
            }
            long completeEnd = fileLength - remaining;
            int frameIndex = 0;
            while (true) {
                final byte[] record;
                try {
                    record = readFrame(in, remaining, log);
                } catch (CorruptFrameException ex) {
                    // the frame's bytes were all present and consumed, so a skippable snapshot frame can be passed over
                    if (recovery == null || log || frameIndex == 0)
                        throw recoveryStops(recovery, log, ex, completeEnd, fileLength);
                    final long frameLength = (long) FRAME_HEADER_SIZE + ex.payloadLength;
                    remaining -= frameLength;
                    completeEnd += frameLength;
                    frameIndex++;
                    recovery.skippedSnapshotFrames++;
                    continue;
                } catch (IOException ex) {
                    // a frame header that can't be trusted (such as a negative length) leaves no way to find the next
                    if (recovery == null || (!log && frameIndex == 0))
                        throw ex;
                    if (log)
                        recovery.logStopped(completeEnd, fileLength, ex.getMessage());
                    else
                        recovery.snapshotStopped(completeEnd, fileLength, ex.getMessage());
                    break;
                }
                if (record == null) {
                    // a snapshot is only ever replaced whole by an atomic rename, so bytes it can't read are damage
                    // rather than an interrupted append
                    if (!log && remaining > 0) {
                        final String reason = String.format("%d trailing bytes do not form a complete record", remaining);
                        if (recovery == null || frameIndex == 0)
                            throw new IOException(String.format("Corrupt storage file %s: %s", file, reason));
                        recovery.snapshotStopped(completeEnd, fileLength, reason);
                    }
                    break;
                }
                final long frameLength = (long) (log ? LOG_FRAME_HEADER_SIZE : FRAME_HEADER_SIZE) + record.length;
                if (recovery == null) {
                    decodeFrame(record, vertices, edges);
                } else {
                    try {
                        decodeFrameAtomically(record, vertices, edges);
                    } catch (IOException ex) {
                        if (log || frameIndex == 0)
                            throw recoveryStops(recovery, log, ex, completeEnd, fileLength);
                        recovery.skippedSnapshotFrames++;
                    }
                }
                remaining -= frameLength;
                completeEnd += frameLength;
                frameIndex++;
            }
            return completeEnd;
        } catch (RecoveryStop stop) {
            return stop.completeEnd;
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage file %s", file), ex);
        }
    }

    /**
     * Decide what a damaged frame means under recovery. In the log, replay stops there (signalled by the returned
     * {@link RecoveryStop}). In the snapshot's dictionary frame, or with no recovery at all, the damage is rethrown.
     */
    private static IOException recoveryStops(final Recovery recovery, final boolean log, final IOException damage,
                                             final long completeEnd, final long fileLength) {
        if (recovery == null || !log)
            return damage;
        recovery.logStopped(completeEnd, fileLength, damage.getMessage());
        return new RecoveryStop(completeEnd);
    }

    /**
     * Decode a frame so that it is applied entirely or not at all. {@link #decodeFrame} applies a frame's entries one
     * by one, so a frame that fails partway would otherwise leave its first entries behind. That doesn't matter when
     * any damage fails the open, but recovery keeps going and must not keep half of a transaction.
     */
    private void decodeFrameAtomically(final byte[] record, final Map<Object, DetachedVertex> vertices,
                                       final Map<Object, DetachedEdge> edges) throws IOException {
        final JournaledMap<Object, DetachedVertex> v = new JournaledMap<>(vertices);
        final JournaledMap<Object, DetachedEdge> e = new JournaledMap<>(edges);
        try {
            decodeFrame(record, v, e);
        } catch (IOException | RuntimeException ex) {
            e.undo();
            v.undo();
            throw ex instanceof IOException ? (IOException) ex : new IOException(ex.toString(), ex);
        }
    }

    /**
     * Cut the log back to {@code completeEnd}, the end of its last complete frame, so that the next append starts on
     * a frame boundary. Without this, new commits would be written after the torn bytes and a later replay would stop
     * at the old torn frame (dropping them) or read into them (failing the CRC check). Only bytes that never formed a
     * complete frame are removed, and no acknowledged commit is ever an incomplete frame.
     */
    private void truncateTornLogTail(final long completeEnd) {
        final long length = logFile.length();
        if (length <= completeEnd)
            return;
        logger.warn("Storage log {} ends in an interrupted append; discarding its last {} bytes", logFile, length - completeEnd);
        try (final FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.WRITE)) {
            channel.truncate(completeEnd);
            channel.force(true);
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not truncate interrupted append from storage log %s", logFile), ex);
        }
    }

    /**
     * Read and validate the magic at the start of the per-file header, returning the number of bytes that follow the
     * whole header. The generation that completes the header is left for the caller to read.
     */
    private static long readAndVerifyHeader(final DataInputStream in, final File file, final long fileLength) throws IOException {
        if (fileLength < HEADER_SIZE)
            throw new IOException(String.format("Corrupt storage file %s: shorter than its %d-byte header", file, HEADER_SIZE));
        final byte[] magic = new byte[MAGIC.length];
        readFully(in, magic);
        if (!Arrays.equals(magic, MAGIC))
            throw new IOException(String.format("%s is not a TinkerGraph storage file (bad magic)", file));
        return fileLength - HEADER_SIZE;
    }

    private void ensureLogOpen() {
        // never reopen the log of a closed engine: its directory lock is released, so the files may now belong to
        // another graph
        checkNotClosed();
        if (logOut == null) {
            // open and compaction always leave a log in place, so appending to a missing one would write frames with
            // no header
            if (!logFile.isFile())
                throw new IllegalStateException(String.format("Storage log %s is missing", logFile));
            try {
                // retain the FileOutputStream so flush() can reach its FileDescriptor for fsync
                logFos = openLogForAppend(logFile);
                logOut = new DataOutputStream(new BufferedOutputStream(logFos));
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not open storage log for append", ex);
            }
        }
    }

    /**
     * Open the log for appending. Exists so tests can substitute a stream that fails on demand.
     */
    protected FileOutputStream openLogForAppend(final File file) throws IOException {
        return new FileOutputStream(file, true);
    }

    private void closeLog() {
        if (logOut == null)
            return;
        try {
            logOut.flush();
            logOut.close();
        } catch (IOException ex) {
            discardLog();
            throw fail("Could not close storage log", ex);
        }
        logOut = null;
        logFos = null;
    }

    /**
     * Close the log file without writing anything still buffered for it.
     */
    private void discardLog() {
        if (logFos != null) {
            try {
                logFos.close();
            } catch (IOException ignored) {
                // best effort: the log already failed and the next open recovers from what is on disk
            }
        }
        logOut = null;
        logFos = null;
    }

    /**
     * Record the first log write failure, putting the engine into its fail-stop state, and return the exception to
     * throw for this one.
     */
    private UncheckedIOException fail(final String message, final IOException cause) {
        if (failure == null)
            failure = cause;
        return new UncheckedIOException(message + "; no further commits are accepted until the graph is reopened", cause);
    }

    /**
     * Refuse a commit once the engine is closed. The graph sets {@code closed} under its storage commit lock, the same
     * lock {@link #persist} runs under, so a commit that was waiting on {@code close()} fails here instead of writing
     * to a log that is no longer this graph's to write.
     */
    private void checkNotClosed() {
        if (closed)
            throw new IllegalStateException(String.format(
                    "Storage at %s is closed and accepts no further commits; open the graph again", directory));
    }

    private void checkNotRecovering() {
        if (recovering)
            throw new IllegalStateException(String.format(
                    "Storage at %s was opened with %s and is read-only; export the graph with g.io() into a new " +
                            "storage directory", directory, TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
    }

    private void checkNotFailed() {
        if (failure != null)
            throw new UncheckedIOException(new IOException(String.format(
                    "Storage at %s failed an earlier write and accepts no further commits; close and reopen the graph",
                    directory), failure));
    }

    /**
     * Write the per-file header ({@link #MAGIC} and the file's generation) at the start of a storage file.
     */
    private static void writeHeader(final DataOutputStream out, final long fileGeneration) throws IOException {
        out.write(MAGIC);
        out.writeLong(fileGeneration);
    }

    /**
     * Write a snapshot frame: a 4-byte big-endian payload length, a 4-byte CRC32 of the payload, then the payload.
     * The checksum lets a reader tell a bit-flip inside a complete frame from an incomplete one. Available to codec
     * subclasses writing per-element snapshot frames; log frames are written by {@link #writeLogFrame}.
     */
    protected static void writeFrame(final DataOutputStream out, final byte[] payload) throws IOException {
        final CRC32 crc = new CRC32();
        crc.update(payload);
        out.writeInt(payload.length);
        out.writeInt((int) crc.getValue());
        out.write(payload);
    }

    /**
     * Write a log frame: a 4-byte big-endian payload length, a 4-byte CRC32 of that length, a 4-byte CRC32 of the
     * payload, then the payload. See {@link #LOG_FRAME_HEADER_SIZE} for why the length carries its own checksum.
     */
    private static void writeLogFrame(final DataOutputStream out, final byte[] payload) throws IOException {
        final CRC32 crc = new CRC32();
        crc.update(payload);
        out.writeInt(payload.length);
        out.writeInt(lengthCrc(payload.length));
        out.writeInt((int) crc.getValue());
        out.write(payload);
    }

    private static int lengthCrc(final int length) {
        final CRC32 crc = new CRC32();
        crc.update(ByteBuffer.allocate(Integer.BYTES).putInt(length).array());
        return (int) crc.getValue();
    }

    /**
     * Read a framed record, or return {@code null} at end of the readable log. A frame only partially present is
     * treated as an interrupted trailing append (truncation) and ends reading; a fully-present frame whose stored CRC
     * does not match is genuine corruption and is raised. In the log ({@code log} set), a length that fails its own
     * checksum is likewise corruption, so only a verified length that claims more bytes than remain reads as a torn
     * tail.
     */
    private static byte[] readFrame(final DataInputStream in, final long remaining, final boolean log) throws IOException {
        final int headerSize = log ? LOG_FRAME_HEADER_SIZE : FRAME_HEADER_SIZE;
        if (remaining == 0)
            return null; // clean end of file, exactly on a frame boundary
        if (remaining < headerSize)
            return null; // not even a full header left — interrupted append

        final int length = in.readInt();
        if (log) {
            final int storedLengthCrc = in.readInt();
            if (storedLengthCrc != lengthCrc(length))
                throw new IOException(String.format(
                        "Corrupt storage frame: length %d fails its checksum (stored %08x, computed %08x)",
                        length, storedLengthCrc, lengthCrc(length)));
        }
        final int storedCrc = in.readInt();
        if (length < 0)
            throw new IOException("Corrupt storage frame: negative payload length " + length);
        if ((long) length > remaining - headerSize)
            return null; // frame claims more bytes than remain — truncated trailing append

        final byte[] payload = new byte[length];
        try {
            readFully(in, payload);
        } catch (EOFException eof) {
            return null; // partial trailing payload from an interrupted append
        }

        final CRC32 crc = new CRC32();
        crc.update(payload);
        if ((int) crc.getValue() != storedCrc)
            throw new CorruptFrameException(String.format(
                    "Corrupt storage frame: CRC mismatch (stored %08x, computed %08x) in a fully-present %d-byte record",
                    storedCrc, (int) crc.getValue(), length), length);
        return payload;
    }

    private static void readFully(final InputStream in, final byte[] dst) throws IOException {
        int off = 0;
        while (off < dst.length) {
            final int read = in.read(dst, off, dst.length - off);
            if (read < 0)
                throw new EOFException();
            off += read;
        }
    }

    private static void deleteQuietly(final File file) {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException ignored) {
            // best effort: the next compaction overwrites the temporary file anyway
        }
    }

    /**
     * Atomically move {@code source} onto {@code target}, replacing any existing target. Falls back to a non-atomic
     * replacing move on filesystems that do not support atomic moves.
     */
    private static void atomicMove(final File source, final File target) throws IOException {
        try {
            Files.move(source.toPath(), target.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException anse) {
            Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * fsync the storage directory so that recent namespace changes (a rename into place, a file deletion) are durable.
     */
    private void syncDirectory() {
        try (final FileChannel dirChannel = FileChannel.open(directory.toPath(), StandardOpenOption.READ)) {
            dirChannel.force(true);
        } catch (IOException ex) {
            // some platforms (notably Windows) cannot open a directory as a channel; the atomic rename is the
            // durability guarantee there, so treat inability to sync the directory as non-fatal
        }
    }

    /**
     * A fully-present frame whose checksum does not match. Its bytes have been consumed, so a reader that can afford to
     * lose the frame may continue with the next one.
     */
    private static final class CorruptFrameException extends IOException {
        private final int payloadLength;

        private CorruptFrameException(final String message, final int payloadLength) {
            super(message);
            this.payloadLength = payloadLength;
        }
    }

    /**
     * Ends a recovery fold of the log at the last frame that was applied.
     */
    private static final class RecoveryStop extends IOException {
        private final long completeEnd;

        private RecoveryStop(final long completeEnd) {
            super("recovery stopped replaying the log at byte " + completeEnd);
            this.completeEnd = completeEnd;
        }
    }

    /**
     * What a recovery open had to leave behind, reported once replay is done.
     */
    private static final class Recovery {
        private int skippedSnapshotFrames = 0;
        private int danglingEdges = 0;
        private long logStoppedAt = -1;
        private long logLength = 0;
        private String logStopReason;
        private long snapshotStoppedAt = -1;
        private long snapshotLength = 0;
        private String snapshotStopReason;
        private final List<String> notes = new ArrayList<>();

        private void note(final String problem) {
            notes.add(problem);
        }

        private void snapshotStopped(final long offset, final long length, final String reason) {
            this.snapshotStoppedAt = offset;
            this.snapshotLength = length;
            this.snapshotStopReason = reason;
        }

        private void logStopped(final long offset, final long length, final String reason) {
            this.logStoppedAt = offset;
            this.logLength = length;
            this.logStopReason = reason;
        }

        private void report(final File directory) {
            final StringBuilder sb = new StringBuilder(String.format(
                    "Opened storage at %s with %s; the graph is read-only.", directory,
                    TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
            if (notes.isEmpty() && skippedSnapshotFrames == 0 && danglingEdges == 0 && logStoppedAt < 0 &&
                    snapshotStoppedAt < 0)
                sb.append(" No damage was found.");
            for (final String n : notes)
                sb.append(' ').append(n);
            if (skippedSnapshotFrames > 0)
                sb.append(String.format(" Skipped %d unreadable element record(s) in the snapshot.", skippedSnapshotFrames));
            if (snapshotStoppedAt >= 0)
                sb.append(String.format(" Stopped reading the snapshot at byte %d of %d (%s), so any elements after " +
                        "that point are not included.", snapshotStoppedAt, snapshotLength, snapshotStopReason));
            if (logStoppedAt >= 0)
                sb.append(String.format(" Stopped replaying the log at byte %d of %d (%s), so any transactions after " +
                        "that point are not included.", logStoppedAt, logLength, logStopReason));
            if (danglingEdges > 0)
                sb.append(String.format(" Dropped %d edge(s) whose endpoint vertex was lost.", danglingEdges));
            sb.append(" Export the graph with g.io() into a new storage directory.");
            logger.warn(sb.toString());
        }
    }

    /**
     * A map view that records each change it makes to its backing map so that the changes can be undone.
     */
    private static final class JournaledMap<K, V> extends AbstractMap<K, V> {
        private final Map<K, V> backing;
        private final List<Object[]> journal = new ArrayList<>();

        private JournaledMap(final Map<K, V> backing) {
            this.backing = backing;
        }

        @Override
        public V put(final K key, final V value) {
            journal.add(new Object[]{ key, backing.containsKey(key), backing.get(key) });
            return backing.put(key, value);
        }

        @Override
        public V remove(final Object key) {
            if (backing.containsKey(key))
                journal.add(new Object[]{ key, true, backing.get(key) });
            return backing.remove(key);
        }

        @Override
        public V get(final Object key) {
            return backing.get(key);
        }

        @Override
        public boolean containsKey(final Object key) {
            return backing.containsKey(key);
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return Collections.unmodifiableMap(backing).entrySet();
        }

        @SuppressWarnings("unchecked")
        private void undo() {
            for (int i = journal.size() - 1; i >= 0; i--) {
                final Object[] change = journal.get(i);
                if ((Boolean) change[1])
                    backing.put((K) change[0], (V) change[2]);
                else
                    backing.remove(change[0]);
            }
            journal.clear();
        }
    }
}
