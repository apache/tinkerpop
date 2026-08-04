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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * A durable {@link TinkerStorage} engine that persists a {@code TinkerStorageGraph} as an append-only commit log
 * ("write-ahead log") serialized with GraphBinary. On each committed transaction the changeset is appended to
 * {@code log.gbin} as a single record; on open the optional {@code snapshot.gbin} is read followed by the log, with the
 * folded result re-applied to the in-memory graph. {@link #compact(AbstractTinkerGraph)} rewrites the snapshot from the
 * current committed state and truncates the log.
 * <p/>
 * On commit the appended record is made durable according to the configured {@link SyncMode}: {@link SyncMode#COMMIT}
 * (default) {@code fsync}s so the commit survives an OS crash or power loss, while {@link SyncMode#OS} only flushes to
 * the operating system.
 * <p/>
 * On-disk compatibility: the store's format version is recorded once in a {@code VERSION} marker file (magic +
 * version), which is the single source of truth even when a store momentarily holds a snapshot and a log written at
 * different times. Individual files carry only the magic for identity/corruption detection. Opening a store whose
 * marker names an unsupported version fails loudly — records are never misread across a format change. The engine
 * keeps this simple by not attempting in-place migration: the supported path across an incompatible format bump is to
 * export via {@code g.io()} before upgrading. Additive changes should prefer new record opcodes; unknown opcodes are
 * a hard error (a durable store must not silently drop records it cannot parse), so a genuinely incompatible change
 * bumps the version.
 * <p/>
 * The in-memory graph remains authoritative (write-through). This engine does not support graphs larger than memory.
 * <p/>
 * Known limitation (write amplification): a commit records each changed element in full — a single property change on
 * a large element rewrites the whole element to the log. Elements are typically small and automatic compaction bounds
 * the resulting log growth, so this is accepted rather than mitigated with per-property deltas, which would complicate
 * the {@link TinkerStorageMutation} contract and the replay fold. The snapshot, by contrast, is streamed one element
 * at a time so compaction never holds a second full copy of the graph in heap.
 */
public final class GraphBinaryStorage implements TinkerStorage {

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
     * Store-level version marker file, holding {@link #MAGIC} followed by the one-byte {@link #FORMAT_VERSION}. It is
     * the single source of truth for the store's format version.
     */
    static final String VERSION_FILE = "VERSION";

    /**
     * Bytes of the per-file header: just {@link #MAGIC}. The format version lives in the store-level
     * {@link #VERSION_FILE}, not in each file.
     */
    static final int HEADER_SIZE = MAGIC.length;

    private static final byte OP_PUT_VERTEX = 1;
    private static final byte OP_DEL_VERTEX = 2;
    private static final byte OP_PUT_EDGE = 3;
    private static final byte OP_DEL_EDGE = 4;

    static final String SNAPSHOT_FILE = "snapshot.gbin";
    static final String LOG_FILE = "log.gbin";

    private final GraphBinaryWriter writer = new GraphBinaryWriter(TypeSerializerRegistry.INSTANCE);
    private final GraphBinaryReader reader = new GraphBinaryReader(TypeSerializerRegistry.INSTANCE);

    private File directory;
    private File snapshotFile;
    private File logFile;
    private File versionFile;

    /**
     * Default automatic-compaction threshold: 64 MB of appended log since the last compaction.
     */
    static final long DEFAULT_COMPACT_THRESHOLD_BYTES = 64L * 1024 * 1024;

    private DataOutputStream logOut;
    private FileOutputStream logFos;
    private SyncMode syncMode = SyncMode.COMMIT;
    private long compactThresholdBytes = DEFAULT_COMPACT_THRESHOLD_BYTES;
    private long logBytesSinceCompaction = 0;
    private boolean closed = false;

    @Override
    public void open(final AbstractTinkerGraph graph, final Configuration config) {
        final String location = config.getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
        if (null == location)
            throw new IllegalStateException(String.format("%s must be set to use the GraphBinary storage engine",
                    TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION));
        this.directory = new File(location);
        this.snapshotFile = new File(directory, SNAPSHOT_FILE);
        this.logFile = new File(directory, LOG_FILE);
        this.versionFile = new File(directory, VERSION_FILE);
        this.syncMode = SyncMode.fromConfigValue(config.getString(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_SYNC, null));
        this.compactThresholdBytes = config.getLong(
                TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_COMPACT_THRESHOLD, DEFAULT_COMPACT_THRESHOLD_BYTES);
        // seed the counter with any pre-existing log so a graph reopened with a large log still compacts promptly
        this.logBytesSinceCompaction = logFile.exists() ? logFile.length() : 0;
        ensureDirectory();
        establishStoreVersion();
    }

    /**
     * Read and validate the store-level version marker, or create it for a new store. This is the single source of
     * truth for the store's format version: a marker naming an unsupported version, or bad magic, fails the open
     * loudly rather than risking a misread. The supported path across an incompatible format bump is to export the
     * graph via {@code g.io()} before upgrading.
     */
    private void establishStoreVersion() {
        // an existing store (has a snapshot or log) written before the marker existed is treated as version 1
        final boolean storeHasData = snapshotFile.exists() || logFile.exists();
        if (!versionFile.exists()) {
            if (storeHasData && FORMAT_VERSION != 1)
                throw new IllegalStateException(String.format(
                        "Storage location %s has data but no version marker; cannot confirm it is format version %d",
                        directory, FORMAT_VERSION));
            writeStoreVersion();
            return;
        }
        try (final DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(new FileInputStream(versionFile)))) {
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

    /**
     * Write the store-level version marker ({@link #MAGIC} + {@link #FORMAT_VERSION}) durably.
     */
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
     * Ensure the backing directory exists, creating it if necessary. Called on open and again before writing a
     * snapshot, since {@code close()} may be invoked more than once and the directory may have been removed in between.
     */
    private void ensureDirectory() {
        if (directory.exists()) {
            if (!directory.isDirectory())
                throw new IllegalStateException(String.format("Storage location %s exists but is not a directory", directory));
        } else if (!directory.mkdirs()) {
            throw new IllegalStateException(String.format("Could not create storage directory %s", directory));
        }
    }

    @Override
    public void replay(final AbstractTinkerGraph graph) {
        // Fold snapshot then log into final state: last write per id wins, deletes remove.
        final Map<Object, DetachedVertex> vertices = new LinkedHashMap<>();
        final Map<Object, DetachedEdge> edges = new LinkedHashMap<>();

        if (snapshotFile.exists())
            foldRecords(snapshotFile, vertices, edges);
        if (logFile.exists())
            foldRecords(logFile, vertices, edges);

        if (vertices.isEmpty() && edges.isEmpty())
            return;

        // Attach vertices first so edges can find their endpoints, then commit once.
        for (final DetachedVertex v : vertices.values())
            v.attach(Attachable.Method.getOrCreate(graph));
        for (final DetachedEdge e : edges.values())
            e.attach(Attachable.Method.getOrCreate(graph));

        graph.tx().commit();
    }

    /**
     * Read every record in a file, folding puts and deletes into the supplied maps.
     */
    private void foldRecords(final File file, final Map<Object, DetachedVertex> vertices, final Map<Object, DetachedEdge> edges) {
        final long fileLength = file.length();
        try (final DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(new FileInputStream(file)))) {
            long remaining = readAndVerifyHeader(in, file, fileLength);
            while (true) {
                final byte[] record = readFrame(in, remaining);
                if (record == null)
                    break;
                // account for the header (length + crc) and payload just consumed
                remaining -= 2L * Integer.BYTES + record.length;
                applyRecord(record, vertices, edges);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage file %s", file), ex);
        }
    }

    /**
     * Read and validate the per-file header (magic only), returning the number of record bytes that follow it. An
     * empty file (freshly created, no header yet) is treated as having no records. The store's format version is
     * validated once against the {@link #VERSION_FILE} marker in {@link #open}, not here.
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

    private void applyRecord(final byte[] record, final Map<Object, DetachedVertex> vertices, final Map<Object, DetachedEdge> edges) throws IOException {
        // the format version is validated once per file in readAndVerifyHeader, so records no longer repeat it
        final ByteBufferBuffer buffer = new ByteBufferBuffer(record);
        buffer.readLong(); // txVersion, retained for diagnostics/future use
        final int entryCount = buffer.readInt();
        for (int i = 0; i < entryCount; i++) {
            final byte op = buffer.readByte();
            switch (op) {
                case OP_PUT_VERTEX: {
                    final Vertex v = reader.read(buffer);
                    vertices.put(v.id(), (DetachedVertex) v);
                    break;
                }
                case OP_DEL_VERTEX: {
                    final Object id = reader.read(buffer);
                    vertices.remove(id);
                    break;
                }
                case OP_PUT_EDGE: {
                    final Edge e = reader.read(buffer);
                    edges.put(e.id(), (DetachedEdge) e);
                    break;
                }
                case OP_DEL_EDGE: {
                    final Object id = reader.read(buffer);
                    edges.remove(id);
                    break;
                }
                default:
                    throw new IOException("Unknown storage op code: " + op);
            }
        }
    }

    @Override
    public void persist(final long txVersion,
                        final Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                        final Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) {
        ensureLogOpen();
        try {
            final byte[] frame = encodeRecord(txVersion, changedVertices, changedEdges);
            writeFrame(logOut, frame);
            logBytesSinceCompaction += 2L * Integer.BYTES + frame.length; // length + crc prefixes + payload
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not append transaction to storage log", ex);
        }
    }

    /**
     * Serialize a commit record: txVersion, entry count, then each entry as an op byte followed by either the
     * serialized element (put) or the serialized id (delete). The format version lives in the file header, not the
     * record.
     */
    private byte[] encodeRecord(final long txVersion,
                                final Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                                final Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) throws IOException {
        final ByteBufferBuffer buffer = new ByteBufferBuffer();
        buffer.writeLong(txVersion);
        buffer.writeInt(changedVertices.size() + changedEdges.size());
        for (final TinkerStorageMutation<TinkerVertex> m : changedVertices) {
            if (m.isDeleted()) {
                buffer.writeByte(OP_DEL_VERTEX);
                writer.write(m.id(), buffer);
            } else {
                buffer.writeByte(OP_PUT_VERTEX);
                // detach to a stable form independent of the transactional element
                writer.write(DetachedFactory.detach(m.element(), true), buffer);
            }
        }
        for (final TinkerStorageMutation<TinkerEdge> m : changedEdges) {
            if (m.isDeleted()) {
                buffer.writeByte(OP_DEL_EDGE);
                writer.write(m.id(), buffer);
            } else {
                buffer.writeByte(OP_PUT_EDGE);
                writer.write(DetachedFactory.detach(m.element(), true), buffer);
            }
        }
        return buffer.toWrittenArray();
    }

    @Override
    public void flush() {
        if (closed)
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
                throw new UncheckedIOException("Could not flush storage log", ex);
            }
        }
    }

    @Override
    public void compact(final AbstractTinkerGraph graph) {
        if (closed)
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
     * A directory fsync is required because those operations only update the directory entry, which the earlier file
     * fsync does not cover.
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
     * Write the entire current committed state of the graph to {@code out} as a stream of single-element put records,
     * one frame per vertex and per edge. Each frame is an ordinary put record (see {@link #encodeRecord}) with an
     * entry count of one, so {@link #foldRecords} reconstructs the graph from these frames exactly as it would from a
     * commit log. Writing one element at a time keeps peak memory bounded to a single element rather than materializing
     * the whole graph as one byte array, so a snapshot never needs to hold a second full copy of the graph in heap.
     */
    private void writeSnapshot(final AbstractTinkerGraph graph, final DataOutputStream out) throws IOException {
        final Iterator<Vertex> vertexIterator = graph.vertices();
        while (vertexIterator.hasNext())
            writeElementFrame(out, OP_PUT_VERTEX, vertexIterator.next());
        final Iterator<Edge> edgeIterator = graph.edges();
        while (edgeIterator.hasNext())
            writeElementFrame(out, OP_PUT_EDGE, edgeIterator.next());
    }

    /**
     * Encode a single element as a one-entry put record and write it as a framed record to {@code out}. Only one
     * element's bytes are held in memory at a time.
     */
    private void writeElementFrame(final DataOutputStream out, final byte op, final Object element) throws IOException {
        final ByteBufferBuffer buffer = new ByteBufferBuffer();
        buffer.writeLong(0L); // snapshot records have no single tx version
        buffer.writeInt(1);
        buffer.writeByte(op);
        writer.write(DetachedFactory.detach(element, true), buffer);
        writeFrame(out, buffer.toWrittenArray());
    }

    @Override
    public void close() {
        closeLog();
        closed = true;
    }

    private void ensureLogOpen() {
        if (logOut == null) {
            try {
                final boolean freshFile = !logFile.exists() || logFile.length() == 0;
                // retain the FileOutputStream so flush() can reach its FileDescriptor for fsync
                logFos = new FileOutputStream(logFile, true);
                logOut = new DataOutputStream(new BufferedOutputStream(logFos));
                if (freshFile)
                    writeHeader(logOut);
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not open storage log for append", ex);
            }
        }
    }

    /**
     * Write the per-file header ({@link #MAGIC}) at the start of a storage file. The format version is recorded once
     * per store in the {@link #VERSION_FILE} marker, not per file.
     */
    private static void writeHeader(final DataOutputStream out) throws IOException {
        out.write(MAGIC);
    }

    private void closeLog() {
        if (logOut != null) {
            try {
                logOut.flush();
                logOut.close();
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not close storage log", ex);
            } finally {
                logOut = null;
                logFos = null;
            }
        }
    }

    /**
     * Write a framed record: a 4-byte big-endian payload length, a 4-byte CRC32 of the payload, then the payload.
     * The checksum lets a reader tell a bit-flip inside a complete frame (corruption) from a short final frame left
     * by an interrupted append (truncation).
     */
    private static void writeFrame(final DataOutputStream out, final byte[] payload) throws IOException {
        final CRC32 crc = new CRC32();
        crc.update(payload);
        out.writeInt(payload.length);
        out.writeInt((int) crc.getValue());
        out.write(payload);
    }

    /**
     * Read a framed record, or return {@code null} at end of the readable log. A frame that is only partially present
     * — the file ends inside the header or payload — is treated as an interrupted trailing append (truncation) and
     * ends reading so earlier committed records still load. A frame that is fully present but whose stored CRC does
     * not match its payload is genuine corruption and is raised, rather than silently dropping it and everything
     * after it.
     *
     * @param remaining bytes left in the file at the current position; used to distinguish a short trailing frame
     *                  (truncation) from a complete frame, and to bound the payload allocation against a garbage length
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
}
