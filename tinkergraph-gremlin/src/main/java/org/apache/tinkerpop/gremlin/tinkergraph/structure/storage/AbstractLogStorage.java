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
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
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
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * Log-structured durable storage machinery shared by {@link TinkerStorage} engines, independent of how an element is
 * encoded. It persists a {@code TinkerStorageGraph} as an append-only commit log ({@code log.gbin}) plus an optional
 * folded {@code snapshot.gbin}; on open the snapshot is read followed by the log, last-write-wins per element id.
 * <p/>
 * This base owns everything that is not the element codec: the on-disk file layout, the single-source-of-truth
 * {@code VERSION} marker, the store id and compaction generation that tie a log to its snapshot, length+CRC frame
 * framing (which tells an interrupted trailing append apart from genuine corruption, with log frames also checksumming
 * their length), the replay fold loop, durability via {@link SyncMode}, and crash-safe atomic compaction with a size
 * threshold. Concrete engines supply only the codec through {@link #encodeCommit}, {@link #decodeFrame},
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
     * Store-level version marker file, holding {@link #MAGIC}, the one-byte {@link #FORMAT_VERSION} and the store's
     * {@link #STORE_ID_SIZE}-byte id.
     */
    static final String VERSION_FILE = "VERSION";

    /**
     * Bytes of the random id a store is given when it is created: a UUID, as two 8-byte longs. The id is recorded in
     * the {@link #VERSION_FILE} and in every snapshot and log header, so a file copied in from another store is
     * recognized as foreign. The generation alone can't do that, since two stores can be at the same one.
     */
    static final int STORE_ID_SIZE = 2 * Long.BYTES;

    /**
     * Bytes of the per-file header: {@link #MAGIC}, the file's 8-byte compaction generation, the id of the store it
     * belongs to (see {@link #STORE_ID_SIZE}), then a 4-byte CRC32 of those bytes. The format version lives in the
     * store-level {@link #VERSION_FILE}, not in each file. The checksum keeps a damaged header from reading as a
     * believable one: a flipped bit in the generation would otherwise make an intact log look as if it belonged to
     * another snapshot, and a recovery open would then set it aside.
     * <p/>
     * The generation is what lets an open tell a damaged store from a legitimate one. Each compaction writes a snapshot
     * of the next generation and then replaces the log with an empty one of that same generation, so a store with data
     * always has a log, the log's generation always names the snapshot it builds on (or {@code 0} when there is none),
     * and only the crash window between those two renames leaves the snapshot one generation ahead of the log. Any other
     * combination, such as a missing log or a log whose snapshot is gone, means files were lost.
     */
    static final int HEADER_SIZE = MAGIC.length + Long.BYTES + STORE_ID_SIZE + Integer.BYTES;

    /**
     * Bytes ahead of each snapshot frame's payload: a 4-byte length and a 4-byte CRC32 of the payload.
     */
    static final int FRAME_HEADER_SIZE = 2 * Integer.BYTES;

    /**
     * Bytes ahead of each log frame's payload: a 4-byte length, a 4-byte CRC32 of that length, a 4-byte sequence
     * number, and a 4-byte CRC32 of the sequence number and payload together. The log is the only file whose tail is
     * ever cut back, and a length that claims more bytes than remain is what marks a torn tail, so the length must be
     * verifiable on its own. Without that, one corrupted length in the middle of the log would read as a torn tail and
     * every commit after it would be cut off. The sequence number is the frame's position in its log, counting from 0,
     * so a record that was repeated, reordered or copied in from another log is caught rather than replayed. The
     * snapshot is only ever replaced whole, so its frames do without either.
     */
    static final int LOG_FRAME_HEADER_SIZE = 4 * Integer.BYTES;

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

    /**
     * The identity of the log file the open {@link #logOut} writes to (the inode on POSIX), or {@code null} when the
     * filesystem does not report one. On POSIX a removed or replaced file stays writable through a stream that already
     * has it open, so the log is checked against its path after each sync (see {@link #checkStoreInPlace}).
     */
    private Object logFileKey;

    /**
     * The identity of the directory's {@code LOCK} file when the store was opened, or {@code null} when there is none
     * (the engine opened on its own, without a graph holding the directory) or the filesystem reports no identity. The
     * lock held on that file is what keeps a second graph out, and a lock on a file that is no longer at its path keeps
     * out nothing, so the path is checked along with the log's after each sync.
     */
    private Object lockFileKey;

    /**
     * The sequence number the next frame appended to the log carries: the number of frames the log already holds, as
     * counted by replay or reset when a new log is installed.
     */
    private int nextLogSequence = 0;
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
     * The id of this store, read from or written to the {@link #VERSION_FILE} on open, or {@code null} when a recovery
     * open finds no marker to read it from and so can't check which store the files belong to.
     */
    private UUID storeId;

    /**
     * Whether the store's version marker was already present when this open began, which means the store was created
     * by an earlier open and so was given a log then.
     */
    private boolean storeExisted;

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
        if (compactThresholdBytes < 0)
            throw new IllegalArgumentException(String.format(
                    "%s must be 0, to disable automatic compaction, or a positive number of bytes, but was %d",
                    TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, compactThresholdBytes));
        // seed the counter with any pre-existing log so a graph reopened with a large log still compacts promptly
        this.logBytesSinceCompaction = logFile.exists() ? Math.max(0, logFile.length() - HEADER_SIZE) : 0;
        this.recovery = recovering ? new Recovery() : null;
        configureCodec(config);
        ensureDirectory();
        // temporary files are only ever renamed into place once complete, so any left behind belong to a write that
        // was interrupted and hold nothing the store needs
        if (!recovering)
            removeStaleTemporaryFiles();
        establishStoreVersion();
        establishGeneration();
        // remember which file the log is from the start, so a store removed or replaced before the first commit is
        // caught too, not only one replaced while the log is open for append
        if (!recovering) {
            recordLogIdentity();
            recordLockIdentity();
        }
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
        // a graph that has lost its lock may share the directory with another, so it must not add to the log at all
        checkLockIdentity();
        final byte[] frame;
        try {
            frame = encodeCommit(txVersion, changedVertices, changedEdges);
        } catch (IOException ex) {
            // nothing has been written yet, so the log is still sound and only this transaction fails
            throw new UncheckedIOException("Could not encode transaction for storage log: " + ex.getMessage(), ex);
        }
        try {
            writeLogFrame(logOut, nextLogSequence, frame);
        } catch (IOException ex) {
            throw fail("Could not append transaction to storage log", ex);
        }
        nextLogSequence++;
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
            // the sync succeeds even on a log that was removed from the directory, so check the commit actually
            // landed in the store before it is acknowledged
            checkStoreInPlace();
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
        // compaction writes into the directory it was opened on and never recreates it, so a store removed while the
        // graph is open stays removed (the open log is the store's only file the engine holds, so check it first)
        checkStoreInPlace();
        // with nothing logged since the last compaction the snapshot already holds everything the graph has
        // committed, so rewriting it would only cost a full write and sync, which close() would pay every time
        if (logBytesSinceCompaction == 0)
            return;
        closeLog();
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
            StorageFiles.deleteQuietly(tmp);
            throw new UncheckedIOException("Could not write storage snapshot", ex);
        } catch (RuntimeException | Error ex) {
            StorageFiles.deleteQuietly(tmp);
            throw ex;
        }

        try {
            // atomically replace the snapshot; no delete-then-rename window where the snapshot is briefly absent
            try {
                StorageFiles.atomicMove(tmp, snapshotFile);
            } catch (IOException ex) {
                StorageFiles.deleteQuietly(tmp);
                throw ex;
            }
            // fsync the directory so the rename survives a crash before we touch the log
            StorageFiles.syncDirectory(directory);
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
        // the new log is a new file, and from now on it is the one that must still be in place
        recordLogIdentity();

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
    public void clear(final AbstractTinkerGraph graph) {
        checkNotClosed();
        checkNotRecovering();
        checkNotFailed();
        try {
            compact(graph);
        } catch (RuntimeException ex) {
            // compaction is crash-safe, so the store still holds either the old data or nothing; which one is unknown
            // after a failure, and later commits must not be appended to data the graph no longer has. The graph is
            // already empty in memory, so unlike other failures there is nothing left to export.
            final IOException cause = ex.getCause() instanceof IOException ?
                    (IOException) ex.getCause() : new IOException(ex);
            if (failure == null)
                failure = cause;
            throw new UncheckedIOException(String.format("Could not clear storage at %s after the graph was cleared " +
                    "in memory, so the cleared data may return when the graph is reopened; no further commits are " +
                    "accepted until then", directory), cause);
        }
    }

    @Override
    public boolean isReadOnly() {
        return recovering;
    }

    @Override
    public boolean isFailed() {
        return failure != null;
    }

    @Override
    public void failStop(final Throwable cause) {
        if (failure == null)
            failure = new IOException("A transaction recorded in storage could not be applied in memory: " +
                    cause.getMessage(), cause);
    }

    @Override
    public void close() {
        try {
            if (failure instanceof StoreRemovedException) {
                logger.error("Closing storage at {}, which was removed or replaced while the graph was open; nothing " +
                        "more is written to it", directory);
                discardLog();
            } else if (failure != null) {
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
        storeExisted = versionFile.exists();
        if (!versionFile.exists()) {
            if (storeHasData) {
                final String problem = String.format("Storage location %s has data but no %s marker, so its format " +
                        "version cannot be confirmed", directory, VERSION_FILE);
                if (!recovering)
                    throw new IllegalStateException(problem + String.format(
                            "; open it with %s to read it as format version %d",
                            TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER, FORMAT_VERSION));
                recovery.note(problem + String.format("; it was read as format version %d, without checking that " +
                        "its files belong to the same store.", FORMAT_VERSION));
            } else if (!recovering) {
                storeId = UUID.randomUUID();
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
            storeId = new UUID(in.readLong(), in.readLong());
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage version marker %s: %s", versionFile,
                    ex.getMessage()), ex);
        }
    }

    private void writeStoreVersion() {
        // written whole through a temporary file and a rename, so a crash leaves either no marker or a complete one,
        // never a short marker that would fail every later open
        final File tmp = new File(directory, VERSION_FILE + ".tmp");
        try {
            try (final FileOutputStream fos = new FileOutputStream(tmp);
                 final DataOutputStream out = new DataOutputStream(fos)) {
                out.write(MAGIC);
                out.writeByte(FORMAT_VERSION);
                out.writeLong(storeId.getMostSignificantBits());
                out.writeLong(storeId.getLeastSignificantBits());
                out.flush();
                fos.getFD().sync();
            }
            StorageFiles.atomicMove(tmp, versionFile);
            StorageFiles.syncDirectory(directory);
        } catch (IOException ex) {
            StorageFiles.deleteQuietly(tmp);
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
            // a store is given its log when it is created and keeps one from then on, so a store created by an
            // earlier open with neither file has lost its log, and with it every transaction committed before its first
            // compaction. Opening it as a new store would hide that.
            if (storeExisted) {
                final String problem = String.format("Storage log %s is missing and there is no snapshot, so every " +
                        "transaction committed to the store is lost", logFile);
                if (!recovering)
                    throw new IllegalStateException(problem + String.format("; open the store with %s to confirm " +
                            "it is empty, or, if nothing was ever committed to it (its first open was interrupted), " +
                            "delete the directory to start a new store", TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
                recovery.note(problem + "; the graph is empty.");
                return;
            }
            if (!recovering)
                installNewLog(0);
            return;
        }

        final FileHeader snapshotHeader = hasSnapshot ? readHeader(snapshotFile) : null;
        if (snapshotHeader != null && !snapshotHeader.belongsTo(storeId)) {
            // a snapshot and log that agree with each other point at the version marker as the file that is foreign
            final FileHeader otherLog = hasLog ? readHeaderQuietly(logFile) : null;
            if (otherLog != null && snapshotHeader.storeId.equals(otherLog.storeId))
                throw new IllegalStateException(String.format("Storage version marker %s belongs to another store " +
                        "(%s), while snapshot %s and log %s both belong to store %s; if the marker was copied in, " +
                        "remove it and open the store with %s", versionFile, storeId, snapshotFile, logFile,
                        snapshotHeader.storeId, TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
            throw new IllegalStateException(String.format("Storage snapshot %s belongs to another store (%s, not %s)",
                    snapshotFile, snapshotHeader.storeId, storeId));
        }
        final long snapshotGeneration = hasSnapshot ? snapshotHeader.generation : -1;
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

        final FileHeader logHeader;
        try {
            logHeader = readHeader(logFile);
        } catch (UncheckedIOException ex) {
            // a recovery open lets the fold report an unreadable log header and carry on with the snapshot
            if (!recovering)
                throw ex;
            generation = Math.max(0, snapshotGeneration);
            return;
        }

        if (!logHeader.belongsTo(storeId)) {
            final String problem = String.format("Storage log %s belongs to another store (%s, not %s)",
                    logFile, logHeader.storeId, storeId);
            if (!recovering)
                throw new IllegalStateException(problem + String.format("; open the store with %s to read its snapshot",
                        TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_RECOVER));
            recovery.note(problem + "; only the snapshot was read.");
            generation = Math.max(0, snapshotGeneration);
            replayLog = false;
            return;
        }
        final long logGeneration = logHeader.generation;

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
            // compaction wrote the new snapshot but was interrupted before replacing the log, so the snapshot already
            // holds every transaction in the log, and replaying the log over it would add nothing
            replayLog = false;
            if (!recovering) {
                logger.warn("Storage at {} was interrupted while compacting; completing it by emptying the log, " +
                        "whose transactions are all in the new snapshot", directory);
                installNewLog(snapshotGeneration);
                logBytesSinceCompaction = 0;
            } else {
                recovery.note(String.format("Compaction was interrupted before log %s was replaced; the snapshot " +
                        "holds all of its transactions, so the log was not read.", logFile));
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
     * Read the generation and store id from the header of {@code file}, failing if the header is short or is not one
     * of ours.
     */
    private static FileHeader readHeader(final File file) {
        try (final DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            return readAndVerifyHeader(in, file, file.length());
        } catch (IOException ex) {
            // the cause's message says what is wrong, such as a checksum mismatch, so it belongs in this one too
            throw new UncheckedIOException(String.format("Could not read storage file %s: %s", file, ex.getMessage()), ex);
        }
    }

    private static FileHeader readHeaderQuietly(final File file) {
        try {
            return readHeader(file);
        } catch (UncheckedIOException ex) {
            return null;
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
            StorageFiles.deleteQuietly(tmp);
            throw ex;
        }
        try {
            StorageFiles.atomicMove(tmp, logFile);
        } catch (IOException ex) {
            StorageFiles.deleteQuietly(tmp);
            throw ex;
        }
        nextLogSequence = 0;
        StorageFiles.syncDirectory(directory);
    }

    private void removeStaleTemporaryFiles() {
        for (final String name : new String[]{ SNAPSHOT_FILE, LOG_FILE, VERSION_FILE }) {
            final File tmp = new File(directory, name + ".tmp");
            if (tmp.exists()) {
                logger.warn("Removing {}, left by a storage write that was interrupted", tmp);
                StorageFiles.deleteQuietly(tmp);
            }
        }
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
                // the generation and store id were already checked on open
                readAndVerifyHeader(in, file, fileLength);
                remaining = fileLength - HEADER_SIZE;
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
                    record = readFrame(in, remaining, log, frameIndex);
                } catch (CorruptFrameException ex) {
                    // a crash can also leave the last frame's header written but its payload reading back as zeros
                    // where the data never reached the device, which fails the payload's checksum. A final frame that
                    // ends exactly at the end of the log, in a zero byte, is therefore cut as an interrupted append.
                    if (log && completeEnd + LOG_FRAME_HEADER_SIZE + ex.payloadLength == fileLength &&
                            endsInZero(file))
                        break;
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
                    // some filesystems leave a log that crashed during an append longer than what was written, the
                    // extra bytes reading back as zeros. No frame the engine writes starts with a zero header, since
                    // the checksum of length 0 is not 0, so an all-zero rest of the log is never a frame, let alone a
                    // corrupted one: it is cut off like any other interrupted append.
                    if (log && isZeroFrom(file, completeEnd))
                        break;
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
                    try {
                        decodeFrame(record, vertices, edges);
                    } catch (RuntimeException ex) {
                        // reported as the damage it is, naming the file, rather than as an unexplained runtime error
                        throw new IOException(String.format("Corrupt storage frame %d: %s", frameIndex, ex), ex);
                    }
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
            // the frames before completeEnd are all the log keeps, since a torn tail is cut back to it
            if (log)
                nextLogSequence = frameIndex;
            return completeEnd;
        } catch (RecoveryStop stop) {
            return stop.completeEnd;
        } catch (IOException ex) {
            // the cause's message says what is wrong, such as a checksum mismatch, so it belongs in this one too
            throw new UncheckedIOException(String.format("Could not read storage file %s: %s", file, ex.getMessage()), ex);
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
     * Whether the last byte of {@code file} is zero.
     */
    private static boolean endsInZero(final File file) throws IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (raf.length() == 0)
                return false;
            raf.seek(raf.length() - 1);
            return raf.read() == 0;
        }
    }

    /**
     * Whether {@code file} holds at least one byte from {@code offset} on, and every one of them is zero.
     */
    private static boolean isZeroFrom(final File file, final long offset) throws IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (offset >= raf.length())
                return false;
            raf.seek(offset);
            final byte[] chunk = new byte[8192];
            int read;
            while ((read = raf.read(chunk)) > 0) {
                for (int i = 0; i < read; i++) {
                    if (chunk[i] != 0)
                        return false;
                }
            }
            return true;
        }
    }

    /**
     * Cut the log back to {@code completeEnd}, the end of its last complete frame, so that the next append starts on
     * a frame boundary. Without this, new commits would be written after the torn bytes and a later replay would stop
     * at the old torn frame (dropping them) or read into them (failing the CRC check). Only bytes that never formed a
     * complete frame, or that read as zeros past the last complete frame, are removed, and no acknowledged commit is
     * ever either.
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
    private static FileHeader readAndVerifyHeader(final DataInputStream in, final File file, final long fileLength)
            throws IOException {
        if (fileLength < HEADER_SIZE)
            throw new IOException(String.format("Corrupt storage file %s: shorter than its %d-byte header", file, HEADER_SIZE));
        final byte[] header = new byte[HEADER_SIZE];
        readFully(in, header);
        if (!Arrays.equals(Arrays.copyOf(header, MAGIC.length), MAGIC))
            throw new IOException(String.format("%s is not a TinkerGraph storage file (bad magic)", file));
        final ByteBuffer buf = ByteBuffer.wrap(header);
        if (buf.getInt(HEADER_SIZE - Integer.BYTES) != headerCrc(header))
            throw new IOException(String.format("Corrupt storage file %s: its header is damaged (checksum mismatch)",
                    file));
        buf.position(MAGIC.length);
        final long fileGeneration = buf.getLong();
        return new FileHeader(fileGeneration, new UUID(buf.getLong(), buf.getLong()));
    }

    /**
     * The CRC32 of a header's bytes ahead of the checksum itself.
     */
    private static int headerCrc(final byte[] header) {
        final CRC32 crc = new CRC32();
        crc.update(header, 0, HEADER_SIZE - Integer.BYTES);
        return (int) crc.getValue();
    }

    /**
     * The header of a snapshot or log at {@code fileGeneration} in the store with {@code id}.
     */
    static byte[] header(final long fileGeneration, final UUID id) {
        final ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
        buf.put(MAGIC).putLong(fileGeneration)
                .putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
        final byte[] header = buf.array();
        buf.putInt(headerCrc(header));
        return header;
    }

    private void ensureLogOpen() {
        // never reopen the log of a closed engine: its directory lock is released, so the files may now belong to
        // another graph
        checkNotClosed();
        if (logOut == null) {
            // open and compaction always leave a log in place, so appending to a missing one would write frames with
            // no header
            if (!logFile.isFile())
                throw removed(String.format("its log %s is missing", logFile));
            // the log may have been replaced since open or the last compaction, while it was not held open
            checkLogIdentity();
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
     * Fail-stop if the store was removed or replaced since the log was opened: the directory or log is gone, or the
     * log's path now names a different file than the one being written. A commit synced to such a log would be
     * acknowledged yet lost, and a second graph could create the store again beside this one.
     */
    private void checkStoreInPlace() {
        if (!directory.isDirectory())
            throw removed(String.format("its directory %s is missing", directory));
        // open and compaction always leave a log in place, so a missing one was removed whether or not it is open
        if (!logFile.exists())
            throw removed(String.format("its log %s is missing", logFile));
        checkLogIdentity();
        checkLockIdentity();
    }

    private void recordLockIdentity() {
        final File lockFile = new File(directory, DirectoryLock.LOCK_FILE);
        try {
            lockFileKey = lockFile.exists() ? fileKey(lockFile) : null;
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage lock %s", lockFile), ex);
        }
    }

    /**
     * Fail-stop if the {@code LOCK} file the graph holds was removed or replaced, since another graph can then lock a
     * new file at that path and open the store beside this one.
     */
    private void checkLockIdentity() {
        if (null == lockFileKey)
            return;
        final File lockFile = new File(directory, DirectoryLock.LOCK_FILE);
        final Object current;
        try {
            current = fileKey(lockFile);
        } catch (IOException ex) {
            throw removed(String.format("its lock %s is missing", lockFile));
        }
        if (!lockFileKey.equals(current))
            throw removed(String.format("its lock %s was replaced by another file", lockFile));
    }

    /**
     * Remember the file now at the log's path as the store's log. Platforms whose files have no identity key record
     * nothing, and the identity checks then pass.
     */
    private void recordLogIdentity() {
        try {
            logFileKey = logFile.exists() ? fileKey(logFile) : null;
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage log %s", logFile), ex);
        }
    }

    /**
     * Fail-stop if the log's path now names a different file than the one recorded as the store's log.
     */
    private void checkLogIdentity() {
        if (null == logFileKey)
            return;
        final Object current;
        try {
            current = fileKey(logFile);
        } catch (IOException ex) {
            throw removed(String.format("its log %s cannot be read: %s", logFile, ex.getMessage()));
        }
        if (!logFileKey.equals(current))
            throw removed(String.format("its log %s was replaced by another file", logFile));
    }

    private static Object fileKey(final File file) throws IOException {
        return Files.readAttributes(file.toPath(), BasicFileAttributes.class).fileKey();
    }

    /**
     * Fail-stop because the store was removed or replaced while the graph was open. Nothing more is written to the
     * directory, close() included, and the graph's committed data remains readable in memory until it is closed.
     */
    private UncheckedIOException removed(final String what) {
        final StoreRemovedException ex = new StoreRemovedException(String.format(
                "Storage at %s was removed or replaced while the graph was open (%s), so no further commits are " +
                        "accepted. The committed data is still readable in memory: export it with g.io() before " +
                        "closing the graph", directory, what));
        if (failure == null)
            failure = ex;
        return new UncheckedIOException(ex.getMessage(), ex);
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
        if (failure instanceof StoreRemovedException)
            throw new UncheckedIOException(failure.getMessage(), failure);
        if (failure != null)
            throw new UncheckedIOException(new IOException(String.format(
                    "Storage at %s stopped accepting commits after an earlier failure; close and reopen the graph",
                    directory), failure));
    }

    /**
     * Write the per-file header ({@link #MAGIC}, the file's generation and the store id) at the start of a storage
     * file.
     */
    private void writeHeader(final DataOutputStream out, final long fileGeneration) throws IOException {
        out.write(header(fileGeneration, storeId));
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
     * Write a log frame: a 4-byte big-endian payload length, a 4-byte CRC32 of that length, the 4-byte sequence number,
     * a 4-byte CRC32 of the sequence number and payload, then the payload. See {@link #LOG_FRAME_HEADER_SIZE} for what
     * the length's own checksum and the sequence number are for.
     */
    private static void writeLogFrame(final DataOutputStream out, final int sequence, final byte[] payload)
            throws IOException {
        out.writeInt(payload.length);
        out.writeInt(lengthCrc(payload.length));
        out.writeInt(sequence);
        out.writeInt(logPayloadCrc(sequence, payload));
        out.write(payload);
    }

    private static int logPayloadCrc(final int sequence, final byte[] payload) {
        final CRC32 crc = new CRC32();
        crc.update(ByteBuffer.allocate(Integer.BYTES).putInt(sequence).array());
        crc.update(payload);
        return (int) crc.getValue();
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
     * tail, and an intact frame whose sequence number is not {@code frameIndex} is raised as one that does not belong
     * where it is.
     */
    private static byte[] readFrame(final DataInputStream in, final long remaining, final boolean log,
                                    final int frameIndex) throws IOException {
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
        final int sequence = log ? in.readInt() : 0;
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

        final int computedCrc;
        if (log) {
            computedCrc = logPayloadCrc(sequence, payload);
        } else {
            final CRC32 crc = new CRC32();
            crc.update(payload);
            computedCrc = (int) crc.getValue();
        }
        if (computedCrc != storedCrc)
            throw new CorruptFrameException(String.format(
                    "Corrupt storage frame: CRC mismatch (stored %08x, computed %08x) in a fully-present %d-byte record",
                    storedCrc, computedCrc, length), length);
        // an intact frame out of place was put there, since the engine only ever appends the next one
        if (log && sequence != frameIndex)
            throw new IOException(String.format("Storage log record %d is numbered %d, so a record was repeated, " +
                    "reordered or copied in from another log", frameIndex, sequence));
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
     * The generation and store id read from a storage file's header.
     */
    private static final class FileHeader {
        private final long generation;
        private final UUID storeId;

        private FileHeader(final long generation, final UUID storeId) {
            this.generation = generation;
            this.storeId = storeId;
        }

        /**
         * Whether the file belongs to the store with {@code id}. Always true when the store's id is unknown.
         */
        private boolean belongsTo(final UUID id) {
            return null == id || id.equals(storeId);
        }
    }

    /**
     * The store was removed or replaced while the graph was open.
     */
    private static final class StoreRemovedException extends IOException {
        private StoreRemovedException(final String message) {
            super(message);
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
