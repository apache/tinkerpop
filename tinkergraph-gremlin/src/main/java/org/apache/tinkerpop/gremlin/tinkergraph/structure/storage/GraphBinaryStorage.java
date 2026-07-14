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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A durable {@link TinkerStorage} engine that persists a {@code TinkerStorageGraph} as an append-only commit log
 * ("write-ahead log") serialized with GraphBinary. On each committed transaction the changeset is appended to
 * {@code log.gbin} as a single record; on open the optional {@code snapshot.gbin} is read followed by the log, with the
 * folded result re-applied to the in-memory graph. {@link #compact(AbstractTinkerGraph)} rewrites the snapshot from the
 * current committed state and truncates the log.
 * <p/>
 * The in-memory graph remains authoritative (write-through). This engine does not support graphs larger than memory.
 */
public final class GraphBinaryStorage implements TinkerStorage {

    /**
     * Version byte prefixing every record, allowing the on-disk format to evolve.
     */
    static final byte FORMAT_VERSION = 1;

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

    private DataOutputStream logOut;
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
        ensureDirectory();
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
        try (final DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(new FileInputStream(file)))) {
            while (true) {
                final byte[] record;
                try {
                    record = readFrame(in);
                } catch (EOFException eof) {
                    break;
                }
                if (record == null)
                    break;
                applyRecord(record, vertices, edges);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(String.format("Could not read storage file %s", file), ex);
        }
    }

    private void applyRecord(final byte[] record, final Map<Object, DetachedVertex> vertices, final Map<Object, DetachedEdge> edges) throws IOException {
        final ByteBufferBuffer buffer = new ByteBufferBuffer(record);
        final byte version = buffer.readByte();
        if (version != FORMAT_VERSION)
            throw new IOException(String.format("Unsupported storage record version %d (expected %d)", version, FORMAT_VERSION));
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
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not append transaction to storage log", ex);
        }
    }

    /**
     * Serialize a commit record: version byte, txVersion, entry count, then each entry as an op byte followed by
     * either the serialized element (put) or the serialized id (delete).
     */
    private byte[] encodeRecord(final long txVersion,
                                final Collection<TinkerStorageMutation<TinkerVertex>> changedVertices,
                                final Collection<TinkerStorageMutation<TinkerEdge>> changedEdges) throws IOException {
        final ByteBufferBuffer buffer = new ByteBufferBuffer();
        buffer.writeByte(FORMAT_VERSION);
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
                logOut.flush();
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not flush storage log", ex);
            }
        }
    }

    @Override
    public void compact(final AbstractTinkerGraph graph) {
        if (closed)
            return;
        // Write a fresh snapshot of the current committed state, then truncate the log.
        closeLog();
        ensureDirectory();
        final File tmp = new File(directory, SNAPSHOT_FILE + ".tmp");
        try (final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmp)))) {
            final byte[] frame = encodeSnapshot(graph);
            if (frame.length > 0)
                writeFrame(out, frame);
            out.flush();
        } catch (IOException ex) {
            throw new UncheckedIOException("Could not write storage snapshot", ex);
        }

        if (snapshotFile.exists() && !snapshotFile.delete())
            throw new UncheckedIOException(new IOException("Could not replace snapshot " + snapshotFile));
        if (!tmp.renameTo(snapshotFile))
            throw new UncheckedIOException(new IOException("Could not rename snapshot into place " + snapshotFile));

        // truncate the log
        if (logFile.exists() && !logFile.delete())
            throw new UncheckedIOException(new IOException("Could not truncate storage log " + logFile));
    }

    /**
     * Serialize the entire current committed state of the graph as a single put-only record.
     */
    private byte[] encodeSnapshot(final AbstractTinkerGraph graph) throws IOException {
        final List<Vertex> vertexList = new ArrayList<>();
        final Iterator<Vertex> vertexIterator = graph.vertices();
        while (vertexIterator.hasNext())
            vertexList.add(vertexIterator.next());
        final List<Edge> edgeList = new ArrayList<>();
        final Iterator<Edge> edgeIterator = graph.edges();
        while (edgeIterator.hasNext())
            edgeList.add(edgeIterator.next());

        if (vertexList.isEmpty() && edgeList.isEmpty())
            return new byte[0];

        final ByteBufferBuffer buffer = new ByteBufferBuffer();
        buffer.writeByte(FORMAT_VERSION);
        buffer.writeLong(0L); // snapshot has no single tx version
        buffer.writeInt(vertexList.size() + edgeList.size());
        for (final Vertex v : vertexList) {
            buffer.writeByte(OP_PUT_VERTEX);
            writer.write(DetachedFactory.detach(v, true), buffer);
        }
        for (final Edge e : edgeList) {
            buffer.writeByte(OP_PUT_EDGE);
            writer.write(DetachedFactory.detach(e, true), buffer);
        }
        return buffer.toWrittenArray();
    }

    @Override
    public void close() {
        closeLog();
        closed = true;
    }

    private void ensureLogOpen() {
        if (logOut == null) {
            try {
                logOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(logFile, true)));
            } catch (IOException ex) {
                throw new UncheckedIOException("Could not open storage log for append", ex);
            }
        }
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
            }
        }
    }

    /**
     * Write a length-prefixed frame: a 4-byte big-endian length followed by the payload.
     */
    private static void writeFrame(final DataOutputStream out, final byte[] payload) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
    }

    /**
     * Read a length-prefixed frame, or return {@code null} on a clean end of file. A truncated final frame (from a
     * crash mid-append) is treated as end of file so earlier committed records still load.
     */
    private static byte[] readFrame(final DataInputStream in) throws IOException {
        final int length;
        try {
            length = in.readInt();
        } catch (EOFException eof) {
            return null;
        }
        if (length < 0)
            throw new IOException("Corrupt storage frame length: " + length);
        final byte[] payload = new byte[length];
        try {
            readFully(in, payload);
        } catch (EOFException eof) {
            // partial trailing frame from an interrupted append — stop here
            return null;
        }
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
