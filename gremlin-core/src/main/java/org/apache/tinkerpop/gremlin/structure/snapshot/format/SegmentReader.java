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
package org.apache.tinkerpop.gremlin.structure.snapshot.format;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Reads the values of a finished segment sequentially, through a small buffer. The header is validated when the
 * reader is opened, including that the file length matches it. Typed reads must match the segment's value width, as
 * for {@link SegmentWriter}. The reader can be repositioned with {@link #seek(long)}.
 * <p/>
 * All I/O failures are reported as {@link UncheckedIOException}. Instances are not thread-safe.
 */
public final class SegmentReader implements AutoCloseable {

    /**
     * The default size of the read buffer in bytes.
     */
    public static final int DEFAULT_BUFFER_BYTES = 64 * 1024;

    private final Path path;
    private final FileChannel channel;
    private final SegmentHeader header;
    private final ByteBuffer buffer;
    private final long endPosition;
    // file position of the next byte to read from the channel into the buffer
    private long channelPosition = SegmentHeader.SIZE;

    private SegmentReader(final Path path, final int bufferBytes) {
        if (bufferBytes < 16) throw new IllegalArgumentException("Buffer too small: " + bufferBytes);
        this.path = path;
        this.buffer = ByteBuffer.allocateDirect(bufferBytes).order(ByteOrder.LITTLE_ENDIAN);
        this.buffer.limit(0);
        try {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
            try {
                this.header = SegmentHeader.read(channel, path);
            } catch (RuntimeException | IOException e) {
                channel.close();
                throw e;
            }
            this.endPosition = header.fileBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Opens a finished segment for sequential reading.
     *
     * @throws UncheckedIOException if the file cannot be read or is not a well-formed segment
     */
    public static SegmentReader open(final Path path) {
        return new SegmentReader(path, DEFAULT_BUFFER_BYTES);
    }

    /**
     * As {@link #open(Path)} with an explicit read buffer size.
     */
    public static SegmentReader open(final Path path, final int bufferBytes) {
        return new SegmentReader(path, bufferBytes);
    }

    public Path path() {
        return path;
    }

    public SegmentHeader header() {
        return header;
    }

    public int valueWidth() {
        return header.valueWidth();
    }

    /**
     * The total number of values in the segment.
     */
    public long count() {
        return header.count();
    }

    /**
     * The index of the next value to be read.
     */
    public long position() {
        return (channelPosition - buffer.remaining() - SegmentHeader.SIZE) / header.valueWidth();
    }

    /**
     * The number of values that remain to be read.
     */
    public long remaining() {
        return header.count() - position();
    }

    public boolean hasRemaining() {
        return remaining() > 0;
    }

    /**
     * Repositions the reader so the next read returns the value at {@code valueIndex}. An index equal to
     * {@link #count()} positions the reader at the end.
     */
    public void seek(final long valueIndex) {
        if (valueIndex < 0 || valueIndex > header.count()) {
            throw new IndexOutOfBoundsException("Index " + valueIndex + " out of range [0, " + header.count() + "]");
        }
        buffer.clear().limit(0);
        channelPosition = SegmentHeader.SIZE + valueIndex * header.valueWidth();
    }

    /**
     * @throws java.io.UncheckedIOException wrapping {@link EOFException} at the end of the segment
     */
    public byte readByte() {
        requireWidth(1);
        ensure(1);
        return buffer.get();
    }

    public short readShort() {
        requireWidth(2);
        ensure(2);
        return buffer.getShort();
    }

    public int readInt() {
        requireWidth(4);
        ensure(4);
        return buffer.getInt();
    }

    public long readLong() {
        requireWidth(8);
        ensure(8);
        return buffer.getLong();
    }

    /**
     * Reads raw payload bytes. The length must be a multiple of the value width for the reader to remain aligned.
     */
    public void readBytes(final byte[] dst, final int offset, final int length) {
        Objects.checkFromIndexSize(offset, length, dst.length);
        int off = offset;
        int remaining = length;
        while (remaining > 0) {
            ensure(1);
            final int n = Math.min(remaining, buffer.remaining());
            buffer.get(dst, off, n);
            off += n;
            remaining -= n;
        }
    }

    public void readBytes(final byte[] dst) {
        readBytes(dst, 0, dst.length);
    }

    /**
     * Recomputes the CRC32C of the payload and compares it with the header, without disturbing the read position.
     *
     * @throws UncheckedIOException if the checksum does not match
     */
    public void verifyChecksum() {
        try {
            final long actual = SegmentHeader.computeChecksum(channel, header.payloadBytes());
            if (actual != header.checksum()) {
                throw SegmentHeader.corrupt(path, "checksum mismatch, header " + header.checksum() + " computed " + actual);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void requireWidth(final int width) {
        if (header.valueWidth() != width) {
            throw new IllegalStateException("Segment " + path + " has value width " + header.valueWidth() + ", not " + width);
        }
    }

    // makes at least n bytes available in the buffer
    private void ensure(final int n) {
        if (buffer.remaining() >= n) return;
        buffer.compact();
        try {
            while (buffer.position() < n) {
                final long left = endPosition - channelPosition;
                if (left <= 0) {
                    buffer.flip();
                    throw new UncheckedIOException(new EOFException("End of segment " + path));
                }
                if (left < buffer.remaining()) buffer.limit((int) (buffer.position() + left));
                final int read = channel.read(buffer, channelPosition);
                if (read < 0) {
                    buffer.flip();
                    throw new UncheckedIOException(new EOFException("Unexpected end of file " + path));
                }
                channelPosition += read;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        buffer.limit(buffer.position()).position(0);
    }
}
