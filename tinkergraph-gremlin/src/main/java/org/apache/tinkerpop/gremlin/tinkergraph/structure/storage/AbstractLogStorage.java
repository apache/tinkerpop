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
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Log-structured durable storage machinery shared by {@link TinkerStorage} engines, independent of how an element is
 * encoded. It persists a {@code TinkerStorageGraph} as an append-only commit log ({@code log.gbin}) plus an optional
 * folded {@code snapshot.gbin}; on open the snapshot is read followed by the log, last-write-wins per element id.
 * <p/>
 * This base owns everything that is not the element codec: the on-disk file layout, the single-source-of-truth
 * {@code VERSION} marker, length+CRC frame framing (which tells an interrupted trailing append apart from genuine
 * corruption), the replay fold loop, durability via {@link SyncMode}, and crash-safe atomic compaction with a
 * size threshold. Concrete engines supply only the codec through {@link #encodeCommit}, {@link #decodeFrame},
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
     * Bytes of the per-file header: just {@link #MAGIC}. The format version lives in the store-level
     * {@link #VERSION_FILE}, not in each file.
     */
    static final int HEADER_SIZE = MAGIC.length;

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
    private boolean closed = false;

    /**
     * The first failure to write or flush the log, or {@code null} while the log is sound. Once set, the engine
     * fail-stops: no further commit is accepted, nothing still buffered is written, and only a reopen (which replays
     * what actually reached disk) recovers. After a failed write or fsync, neither the buffered bytes nor the
     * operating system's view of the file can be trusted, so carrying on could make a failed transaction durable.
     * Accessed only under the graph's storage commit lock.
     */
    private IOException failure;

    // ----------------------------------------------------------------------------------------- codec hooks

    /**
     * Encode a committing transaction's changeset into a single record payload (the framing is added by the caller).
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
     * for compaction. The fixed {@link #MAGIC} header has already been written to {@code out}.
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
        this.syncMode = SyncMode.fromConfigValue(config.getString(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_SYNC, null));
        this.compactThresholdBytes = config.getLong(
                TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, DEFAULT_COMPACT_THRESHOLD_BYTES);
        // seed the counter with any pre-existing log so a graph reopened with a large log still compacts promptly
        this.logBytesSinceCompaction = logFile.exists() ? logFile.length() : 0;
        configureCodec(config);
        ensureDirectory();
        establishStoreVersion();
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
            foldRecords(snapshotFile, vertices, edges, false);
        if (logFile.exists())
            truncateTornLogTail(foldRecords(logFile, vertices, edges, true));

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
        checkNotFailed();
        ensureLogOpen();
        final byte[] frame;
        try {
            frame = encodeCommit(txVersion, changedVertices, changedEdges);
        } catch (IOException ex) {
            // nothing has been written yet, so the log is still sound and only this transaction fails
            throw new UncheckedIOException("Could not encode transaction for storage log", ex);
        }
        try {
            writeFrame(logOut, frame);
        } catch (IOException ex) {
            throw fail("Could not append transaction to storage log", ex);
        }
        logBytesSinceCompaction += 2L * Integer.BYTES + frame.length; // length + crc prefixes + payload
    }

    @Override
    public void flush() {
        if (closed || failure != null)
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
        if (closed || failure != null)
            return;
        // Write a fresh snapshot of the current committed state, then truncate the log. This must be crash-safe: at
        // no point may a crash leave the store without a readable snapshot-or-log covering the committed state.
        // Ordering is write-tmp -> fsync tmp -> atomically rename tmp over the snapshot -> fsync dir (the rename is
        // now durable) -> delete the log -> fsync dir. The old snapshot is only ever replaced by an atomic rename, so
        // a crash at any step leaves either the old (snapshot + log) or the new (snapshot) intact — never neither.
        closeLog();
        ensureDirectory();
        final File tmp = new File(directory, SNAPSHOT_FILE + ".tmp");
        try (final FileOutputStream fos = new FileOutputStream(tmp);
             final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fos))) {
            writeHeader(out);
            writeSnapshot(graph, out);
            out.flush();
            // force the snapshot's bytes to the device before it is renamed into place
            fos.getFD().sync();
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not write storage snapshot", ex);
        }

        try {
            // atomically replace the snapshot; no delete-then-rename window where the snapshot is briefly absent
            atomicMove(tmp, snapshotFile);
            // fsync the directory so the rename survives a crash before we touch the log
            syncDirectory();

            // truncate the log now that the snapshot durably reflects the committed state
            if (logFile.exists() && !logFile.delete())
                throw new IOException("Could not truncate storage log " + logFile);
            // fsync the directory again so the log's removal is durable
            syncDirectory();
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not finalize storage snapshot", ex);
        }

        // the log is now empty; the accumulated state lives in the snapshot
        logBytesSinceCompaction = 0;
    }

    @Override
    public void maybeCompact(final AbstractTinkerGraph graph) {
        if (closed || compactThresholdBytes <= 0)
            return;
        if (logBytesSinceCompaction >= compactThresholdBytes)
            compact(graph);
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
            if (storeHasData && FORMAT_VERSION != 1)
                throw new IllegalStateException(String.format(
                        "Storage location %s has data but no version marker; cannot confirm it is format version %d",
                        directory, FORMAT_VERSION));
            writeStoreVersion();
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
                        "Unsupported storage format version %d at %s (this build writes %d); export via g.io() before upgrading",
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
     * complete frame ends. Anything past that offset is an interrupted trailing append. When {@code allowTornHeader}
     * is set, a file shorter than its header whose bytes are a prefix of {@link #MAGIC} is likewise an interrupted
     * first append and yields an offset of {@code 0}.
     */
    private long foldRecords(final File file, final Map<Object, DetachedVertex> vertices,
                             final Map<Object, DetachedEdge> edges, final boolean allowTornHeader) {
        final long fileLength = file.length();
        try (final DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            if (allowTornHeader && fileLength > 0 && fileLength < HEADER_SIZE) {
                final byte[] present = new byte[(int) fileLength];
                readFully(in, present);
                if (Arrays.equals(present, Arrays.copyOf(MAGIC, present.length)))
                    return 0;
            }
            long remaining = readAndVerifyHeader(in, file, fileLength);
            long completeEnd = fileLength - remaining;
            while (true) {
                final byte[] record = readFrame(in, remaining);
                if (record == null)
                    break;
                final long frameLength = 2L * Integer.BYTES + record.length;
                remaining -= frameLength;
                completeEnd += frameLength;
                decodeFrame(record, vertices, edges);
            }
            return completeEnd;
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage file %s", file), ex);
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
     * Read and validate the per-file header (magic only), returning the number of record bytes that follow it.
     */
    private long readAndVerifyHeader(final DataInputStream in, final File file, final long fileLength) throws IOException {
        if (fileLength == 0)
            return 0;
        if (fileLength < HEADER_SIZE)
            throw new IOException(String.format("Corrupt storage file %s: shorter than its %d-byte header", file, HEADER_SIZE));
        final byte[] magic = new byte[MAGIC.length];
        readFully(in, magic);
        if (!Arrays.equals(magic, MAGIC))
            throw new IOException(String.format("%s is not a TinkerGraph storage file (bad magic)", file));
        return fileLength - HEADER_SIZE;
    }

    private void ensureLogOpen() {
        if (logOut == null) {
            try {
                final boolean freshFile = !logFile.exists() || logFile.length() == 0;
                // retain the FileOutputStream so flush() can reach its FileDescriptor for fsync
                logFos = openLogForAppend(logFile);
                logOut = new DataOutputStream(new BufferedOutputStream(logFos));
                if (freshFile)
                    writeHeader(logOut);
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

    private void checkNotFailed() {
        if (failure != null)
            throw new UncheckedIOException(new IOException(String.format(
                    "Storage at %s failed an earlier write and accepts no further commits; close and reopen the graph",
                    directory), failure));
    }

    /**
     * Write the per-file header ({@link #MAGIC}) at the start of a storage file.
     */
    private static void writeHeader(final DataOutputStream out) throws IOException {
        out.write(MAGIC);
    }

    /**
     * Write a framed record: a 4-byte big-endian payload length, a 4-byte CRC32 of the payload, then the payload.
     * The checksum lets a reader tell a bit-flip inside a complete frame (corruption) from a short final frame left
     * by an interrupted append (truncation). Available to codec subclasses writing per-element snapshot frames.
     */
    protected static void writeFrame(final DataOutputStream out, final byte[] payload) throws IOException {
        final CRC32 crc = new CRC32();
        crc.update(payload);
        out.writeInt(payload.length);
        out.writeInt((int) crc.getValue());
        out.write(payload);
    }

    /**
     * Read a framed record, or return {@code null} at end of the readable log. A frame only partially present is
     * treated as an interrupted trailing append (truncation) and ends reading; a fully-present frame whose stored CRC
     * does not match is genuine corruption and is raised.
     */
    private static byte[] readFrame(final DataInputStream in, final long remaining) throws IOException {
        if (remaining == 0)
            return null; // clean end of file, exactly on a frame boundary
        if (remaining < 2L * Integer.BYTES)
            return null; // not even a full header left — interrupted append

        final int length = in.readInt();
        final int storedCrc = in.readInt();
        if (length < 0)
            throw new IOException("Corrupt storage frame: negative payload length " + length);
        if ((long) length > remaining - 2L * Integer.BYTES)
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
            throw new IOException(String.format(
                    "Corrupt storage frame: CRC mismatch (stored %08x, computed %08x) in a fully-present %d-byte record",
                    storedCrc, (int) crc.getValue(), length));
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
}
