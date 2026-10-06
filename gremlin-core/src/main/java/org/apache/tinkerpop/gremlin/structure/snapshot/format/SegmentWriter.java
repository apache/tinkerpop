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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * Appends values to a new segment file sequentially, through a small buffer. The header is reserved as 64 zero bytes
 * when the writer is created and is filled in, including the count and the CRC32C checksum, only by {@link #finish()}
 * or {@link #close()}. A segment that was never finished therefore fails the magic check when opened.
 * <p/>
 * Typed writes must match the segment's value width: {@link #writeByte} for width 1, {@link #writeShort} for 2,
 * {@link #writeInt} for 4, {@link #writeLong} for 8. Width-16 segments and raw payloads use {@link #writeBytes}.
 * Values are little-endian. Variable-width payloads such as {@code data.bin} are width-1 segments.
 * <p/>
 * All I/O failures are reported as {@link UncheckedIOException}. Instances are not thread-safe.
 */
public final class SegmentWriter implements AutoCloseable {

    /**
     * The default size of the write buffer in bytes.
     */
    public static final int DEFAULT_BUFFER_BYTES = 64 * 1024;

    private final Path path;
    private final int valueWidth;
    private final FileChannel channel;
    private final ByteBuffer buffer;
    private final CRC32C crc = new CRC32C();
    private long payloadBytes;
    private long channelPosition = SegmentHeader.SIZE;
    private SegmentHeader header;

    private SegmentWriter(final Path path, final int valueWidth, final int bufferBytes) {
        if (!SegmentHeader.isValidWidth(valueWidth)) throw new IllegalArgumentException("Invalid value width: " + valueWidth);
        if (bufferBytes < 16) throw new IllegalArgumentException("Buffer too small: " + bufferBytes);
        this.path = path;
        this.valueWidth = valueWidth;
        this.buffer = ByteBuffer.allocateDirect(bufferBytes).order(ByteOrder.LITTLE_ENDIAN);
        try {
            final Path parent = path.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            // reserve the header; it is all zero, and so invalid, until finish()
            channel.write(ByteBuffer.allocate(SegmentHeader.SIZE), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates (or truncates) the segment file at {@code path}, creating missing parent directories.
     *
     * @param valueWidth the value width in bytes: 1, 2, 4, 8 or 16
     */
    public static SegmentWriter create(final Path path, final int valueWidth) {
        return new SegmentWriter(path, valueWidth, DEFAULT_BUFFER_BYTES);
    }

    /**
     * As {@link #create(Path, int)} with an explicit write buffer size.
     */
    public static SegmentWriter create(final Path path, final int valueWidth, final int bufferBytes) {
        return new SegmentWriter(path, valueWidth, bufferBytes);
    }

    public Path path() {
        return path;
    }

    public int valueWidth() {
        return valueWidth;
    }

    /**
     * The number of whole values written so far, which is the index the next value will have.
     */
    public long count() {
        return payloadBytes / valueWidth;
    }

    public void writeByte(final int value) {
        requireWidth(1);
        room(1);
        buffer.put((byte) value);
        payloadBytes += 1;
    }

    public void writeShort(final int value) {
        requireWidth(2);
        room(2);
        buffer.putShort((short) value);
        payloadBytes += 2;
    }

    public void writeInt(final int value) {
        requireWidth(4);
        room(4);
        buffer.putInt(value);
        payloadBytes += 4;
    }

    public void writeLong(final long value) {
        requireWidth(8);
        room(8);
        buffer.putLong(value);
        payloadBytes += 8;
    }

    /**
     * Appends raw payload bytes. The total number of payload bytes must be a multiple of the value width by the time
     * the segment is finished.
     */
    public void writeBytes(final byte[] src) {
        writeBytes(src, 0, src.length);
    }

    /**
     * Appends raw payload bytes. The total number of payload bytes must be a multiple of the value width by the time
     * the segment is finished.
     */
    public void writeBytes(final byte[] src, final int offset, final int length) {
        java.util.Objects.checkFromIndexSize(offset, length, src.length);
        int off = offset;
        int remaining = length;
        while (remaining > 0) {
            if (!buffer.hasRemaining()) flushBuffer();
            final int n = Math.min(remaining, buffer.remaining());
            buffer.put(src, off, n);
            off += n;
            remaining -= n;
        }
        payloadBytes += length;
    }

    /**
     * Writes the header, including the checksum, and closes the file. Idempotent.
     *
     * @return the finalized header
     * @throws IllegalStateException if the payload length is not a multiple of the value width
     */
    public SegmentHeader finish() {
        if (header != null) return header;
        try {
            flushBuffer();
            if (payloadBytes % valueWidth != 0) {
                channel.close();
                throw new IllegalStateException("Segment " + path + " has " + payloadBytes
                        + " payload bytes, not a multiple of value width " + valueWidth);
            }
            header = new SegmentHeader(valueWidth, payloadBytes / valueWidth, crc.getValue());
            final ByteBuffer h = header.toBuffer();
            while (h.hasRemaining()) channel.write(h, h.position());
            channel.close();
            return header;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The finalized header. Valid only after {@link #finish()} or {@link #close()}.
     */
    public SegmentHeader header() {
        if (header == null) throw new IllegalStateException("Segment " + path + " is not finished");
        return header;
    }

    /**
     * The checksum of the payload. Valid only after {@link #finish()} or {@link #close()}.
     */
    public long checksum() {
        return header().checksum();
    }

    /**
     * Describes the finished segment for the manifest. Valid only after {@link #finish()} or {@link #close()}.
     *
     * @param relativePath the path relative to the bundle root, using {@code '/'} separators
     */
    public Manifest.SegmentInfo info(final String relativePath) {
        final SegmentHeader h = header();
        return new Manifest.SegmentInfo(relativePath, h.valueWidth(), h.count(), h.checksum());
    }

    /**
     * Finishes the segment if {@link #finish()} has not been called.
     */
    @Override
    public void close() {
        finish();
    }

    private void requireWidth(final int width) {
        if (valueWidth != width) {
            throw new IllegalStateException("Segment " + path + " has value width " + valueWidth + ", not " + width);
        }
    }

    private void room(final int bytes) {
        if (buffer.remaining() < bytes) flushBuffer();
    }

    private void flushBuffer() {
        if (buffer.position() == 0) return;
        buffer.flip();
        crc.update(buffer.duplicate());
        try {
            while (buffer.hasRemaining()) channelPosition += channel.write(buffer, channelPosition);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        buffer.clear();
    }
}
