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
package org.apache.tinkerpop.gremlin.structure.snapshot.build;

import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueCodec;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueType;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;

/**
 * Reads back the records of a finished {@link PropertySpool} sequentially. The caller knows the record layout, which is
 * described by {@link SpoolRecord}.
 */
final class PropertySpoolReader implements AutoCloseable {

    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    /**
     * A reusable holder for the payload of one encoded value.
     */
    static final class Payload {
        byte[] bytes = new byte[64];
        int length;
    }

    // a file spool is read through the segment reader; a spool held in heap is read from its chunks
    private final SegmentReader reader;
    private final List<byte[]> chunks;
    private final int chunkBytes;
    private final long length;
    private long position;
    private final byte[] scratch = new byte[8];

    PropertySpoolReader(final Path path, final int bufferBytes) {
        this.reader = SegmentReader.open(path, bufferBytes);
        this.chunks = null;
        this.chunkBytes = 0;
        this.length = 0;
    }

    private PropertySpoolReader(final List<byte[]> chunks, final int chunkBytes, final long length) {
        this.reader = null;
        this.chunks = chunks;
        this.chunkBytes = chunkBytes;
        this.length = length;
    }

    /**
     * A reader over the records of a spool that is held in heap.
     *
     * @param chunks     the chunks in order, all but the last full
     * @param chunkBytes the size of a chunk
     * @param length     the number of bytes written
     */
    static PropertySpoolReader ofMemory(final List<byte[]> chunks, final int chunkBytes, final long length) {
        return new PropertySpoolReader(chunks, chunkBytes, length);
    }

    boolean hasRemaining() {
        return reader != null ? reader.hasRemaining() : position < length;
    }

    private byte nextByte() {
        if (reader != null) return reader.readByte();
        if (position >= length) throw new IllegalStateException("Corrupt spool: read past the end");
        final byte b = chunks.get((int) (position / chunkBytes))[(int) (position % chunkBytes)];
        position++;
        return b;
    }

    private void nextBytes(final byte[] dst, final int offset, final int count) {
        if (reader != null) {
            reader.readBytes(dst, offset, count);
            return;
        }
        if (length - position < count) throw new IllegalStateException("Corrupt spool: read past the end");
        int off = offset;
        int remaining = count;
        while (remaining > 0) {
            final byte[] chunk = chunks.get((int) (position / chunkBytes));
            final int within = (int) (position % chunkBytes);
            final int n = Math.min(remaining, chunkBytes - within);
            System.arraycopy(chunk, within, dst, off, n);
            position += n;
            off += n;
            remaining -= n;
        }
    }

    int readInt() {
        nextBytes(scratch, 0, 4);
        return (int) INT.get(scratch, 0);
    }

    long readLong() {
        nextBytes(scratch, 0, 8);
        return (long) LONG.get(scratch, 0);
    }

    /**
     * Reads the type code of an encoded value. A null value reads as {@link ValueType#NULL} and has no payload, so the
     * caller must not call {@link #readPayload} for it.
     */
    ValueType readType() {
        final ValueType type = ValueCodec.typeOfCode(nextByte());
        if (type == null) throw new IllegalStateException("Corrupt spool " + describe() + ": invalid type code");
        return type;
    }

    /**
     * Reads the length, when the type is variable-width, and the payload of a value whose type was just read.
     */
    void readPayload(final ValueType type, final Payload into) {
        final int length = type.isFixedWidth() ? type.width() : readInt();
        if (length < 0) throw new IllegalStateException("Corrupt spool " + describe() + ": negative length");
        if (into.bytes.length < length) into.bytes = new byte[Math.max(length, into.bytes.length * 2)];
        nextBytes(into.bytes, 0, length);
        into.length = length;
    }

    private String describe() {
        return reader != null ? String.valueOf(reader.path()) : "(in heap)";
    }

    @Override
    public void close() {
        if (reader != null) reader.close();
    }
}
