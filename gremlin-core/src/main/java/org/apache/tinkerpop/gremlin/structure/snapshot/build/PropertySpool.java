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

import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentHeader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * An append-only scratch spool of variable-length records, stored as a width-1 segment with a regular
 * {@link SegmentHeader} so that {@link PropertySpoolReader} can read it back through a
 * {@link org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentReader}. The header is reserved as zero bytes
 * when the first bytes are flushed and filled in, with the byte count and the checksum, by {@link #finish()}.
 * <p/>
 * Unlike a {@link org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentWriter}, a spool costs only a small heap
 * buffer when it is idle, because by default it holds no open file: the file is opened in append mode for each flush.
 * That lets a build keep one spool per property key open without exhausting file handles or the memory budget. A spool
 * that is written continuously can request a persistent channel instead.
 * <p/>
 * The spool is not thread-safe. I/O failures are reported as {@link UncheckedIOException}.
 */
final class PropertySpool implements AutoCloseable, BuildBudget.Spillable {

    // the unit in which a spool held in heap reserves budget
    static final int CHUNK_BYTES = 64 * 1024;

    private final Path path;
    private final int bufferBytes;
    private byte[] buffer;
    private final boolean keepOpen;
    private final BuildBudget budget;
    // while in heap, the records are in chunks and nothing is written to the file
    private boolean inMemory;
    private List<byte[]> chunks;
    private long memoryBytes;
    private long heldBytes;
    private final CRC32C crc = new CRC32C();
    private FileChannel channel;
    private int position;
    private long payloadBytes;
    private long records;
    private boolean created;
    private boolean finished;

    /**
     * @param path        the spool file, whose parent directory must exist
     * @param bufferBytes the size of the heap buffer; a record larger than the buffer is written straight through
     * @param keepOpen    whether to keep the file open until {@link #finish()} instead of for each flush only
     */
    PropertySpool(final Path path, final int bufferBytes, final boolean keepOpen) {
        this(path, bufferBytes, keepOpen, BuildBudget.none());
    }

    /**
     * A spool that stays in heap while the budget grants it chunks and moves to the file for good once it does not, or
     * when the budget evicts it as the largest holder.
     *
     * @param budget the budget to reserve chunks from; with a budget that grants nothing the spool is file-backed from
     *               the start
     */
    PropertySpool(final Path path, final int bufferBytes, final boolean keepOpen, final BuildBudget budget) {
        this.path = path;
        this.bufferBytes = bufferBytes;
        this.keepOpen = keepOpen;
        this.budget = budget;
        this.inMemory = budget.total() > 0;
        if (inMemory) {
            this.chunks = new ArrayList<>();
            budget.register(this);
        } else {
            this.buffer = new byte[bufferBytes];
        }
    }

    /**
     * Whether the records are held in heap.
     */
    boolean isInMemory() {
        return inMemory;
    }

    @Override
    public long heldBytes() {
        return heldBytes;
    }

    /**
     * Writes the records held in heap to the file and continues there.
     */
    @Override
    public void spill() {
        if (!inMemory) return;
        inMemory = false;
        final boolean wasFinished = finished;
        finished = false;
        buffer = new byte[bufferBytes];
        position = 0;
        final List<byte[]> held = chunks;
        final long bytes = memoryBytes;
        chunks = null;
        memoryBytes = 0;
        budget.unregister(this);
        long remaining = bytes;
        for (final byte[] chunk : held) {
            final int n = (int) Math.min(CHUNK_BYTES, remaining);
            if (n > 0) append(chunk, 0, n);
            remaining -= n;
        }
        budget.release(heldBytes);
        heldBytes = 0;
        if (wasFinished) finish();
    }

    /**
     * Opens the finished spool for reading, from heap when it is there.
     */
    PropertySpoolReader reader(final int readBufferBytes) {
        if (!finished) throw new IllegalStateException("Spool " + path + " is not finished");
        return inMemory ? PropertySpoolReader.ofMemory(chunks, CHUNK_BYTES, memoryBytes)
                : new PropertySpoolReader(path, readBufferBytes);
    }

    /**
     * Frees the heap the spool holds and returns it to the budget. The spool can no longer be read from heap.
     */
    void release() {
        if (!inMemory) return;
        inMemory = false;
        budget.unregister(this);
        chunks = null;
        memoryBytes = 0;
        budget.release(heldBytes);
        heldBytes = 0;
    }

    private boolean appendMemory(final byte[] src, final int length) {
        final long capacity = (long) chunks.size() * CHUNK_BYTES;
        if (memoryBytes + length > capacity) {
            final long need = (memoryBytes + length - capacity + CHUNK_BYTES - 1) / CHUNK_BYTES;
            if (!budget.reserve(need * CHUNK_BYTES, this)) {
                spill();
                return false;
            }
            heldBytes += need * CHUNK_BYTES;
            for (long i = 0; i < need; i++) chunks.add(new byte[CHUNK_BYTES]);
        }
        int offset = 0;
        while (offset < length) {
            final byte[] chunk = chunks.get((int) (memoryBytes / CHUNK_BYTES));
            final int within = (int) (memoryBytes % CHUNK_BYTES);
            final int n = Math.min(length - offset, CHUNK_BYTES - within);
            System.arraycopy(src, offset, chunk, within, n);
            memoryBytes += n;
            offset += n;
        }
        return true;
    }

    Path path() {
        return path;
    }

    /**
     * The number of records appended.
     */
    long records() {
        return records;
    }

    void write(final SpoolRecord record) {
        if (finished) throw new IllegalStateException("Spool " + path + " is finished");
        final byte[] src = record.bytes();
        final int length = record.length();
        records++;
        if (inMemory && appendMemory(src, length)) return;
        if (length > buffer.length - position) {
            flush();
            if (length >= buffer.length) {
                append(src, 0, length);
                return;
            }
        }
        System.arraycopy(src, 0, buffer, position, length);
        position += length;
    }

    private void flush() {
        if (position == 0) return;
        append(buffer, 0, position);
        position = 0;
    }

    private void append(final byte[] src, final int offset, final int length) {
        try {
            final boolean temporary = channel == null;
            final FileChannel ch = temporary ? openChannel() : channel;
            try {
                final ByteBuffer bb = ByteBuffer.wrap(src, offset, length);
                while (bb.hasRemaining()) ch.write(bb);
            } finally {
                if (temporary && !keepOpen) ch.close();
            }
            if (temporary && keepOpen) channel = ch;
            crc.update(src, offset, length);
            payloadBytes += length;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FileChannel openChannel() throws IOException {
        final FileChannel ch = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        if (!created) {
            // reserve the header; it stays all zero, and so invalid, until finish()
            final ByteBuffer header = ByteBuffer.allocate(SegmentHeader.SIZE);
            while (header.hasRemaining()) ch.write(header);
            created = true;
        }
        return ch;
    }

    /**
     * Flushes the spool and writes its header. Idempotent.
     */
    void finish() {
        if (finished) return;
        if (inMemory) {
            finished = true;
            return;
        }
        flush();
        try {
            if (!created) {
                // nothing was written; still produce a valid, empty segment
                openChannel().close();
            }
            if (channel != null) {
                channel.close();
                channel = null;
            }
            try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
                final ByteBuffer header = new SegmentHeader(1, payloadBytes, crc.getValue()).toBuffer();
                while (header.hasRemaining()) ch.write(header, header.position());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finished = true;
    }

    /**
     * Finishes the spool if it is not finished.
     */
    @Override
    public void close() {
        finish();
        release();
    }
}
