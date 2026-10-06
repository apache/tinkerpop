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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * The 64-byte header at the start of every {@code .bin} segment, including scratch spools. All multi-byte fields are
 * little-endian.
 *
 * <pre>
 * offset size field
 *      0    8 magic, ASCII TPCSRSEG
 *      8    4 format version
 *     12    1 byte order, 0 for little-endian
 *     13    3 reserved, zero
 *     16    4 value width in bytes: 1, 2, 4, 8 or 16
 *     20    4 reserved, zero
 *     24    8 value count
 *     32    8 CRC32C of the payload, in the low 32 bits
 *     40   24 reserved, zero
 * </pre>
 *
 * The payload starts at byte {@link #SIZE}. The file length of a well-formed segment is exactly
 * {@code SIZE + count * width}. A header that has not been finalized is all zero, so it fails the magic check.
 */
public final class SegmentHeader {

    /**
     * Header size in bytes, and the file offset of the payload.
     */
    public static final int SIZE = 64;

    /**
     * The magic value at offset 0.
     */
    public static final byte[] MAGIC = "TPCSRSEG".getBytes(StandardCharsets.US_ASCII);

    /**
     * The value of the byte-order field for little-endian, the only value written.
     */
    public static final byte LITTLE_ENDIAN = 0;

    private static final int VERSION_OFFSET = 8;
    private static final int BYTE_ORDER_OFFSET = 12;
    private static final int WIDTH_OFFSET = 16;
    private static final int COUNT_OFFSET = 24;
    private static final int CHECKSUM_OFFSET = 32;

    private final int valueWidth;
    private final long count;
    private final long checksum;

    /**
     * @param valueWidth the value width in bytes: 1, 2, 4, 8 or 16
     * @param count      the number of values in the payload
     * @param checksum   the CRC32C of the payload in the low 32 bits
     */
    public SegmentHeader(final int valueWidth, final long count, final long checksum) {
        if (!isValidWidth(valueWidth)) throw new IllegalArgumentException("Invalid value width: " + valueWidth);
        if (count < 0) throw new IllegalArgumentException("Negative value count: " + count);
        this.valueWidth = valueWidth;
        this.count = count;
        this.checksum = checksum & 0xFFFFFFFFL;
    }

    public int valueWidth() {
        return valueWidth;
    }

    public long count() {
        return count;
    }

    /**
     * The stored CRC32C of the payload, in the range {@code [0, 2^32)}.
     */
    public long checksum() {
        return checksum;
    }

    /**
     * The payload length in bytes, {@code count * valueWidth}.
     *
     * @throws ArithmeticException if the length overflows a long
     */
    public long payloadBytes() {
        return Math.multiplyExact(count, (long) valueWidth);
    }

    /**
     * The expected length of the whole file in bytes.
     */
    public long fileBytes() {
        return Math.addExact(SIZE, payloadBytes());
    }

    public static boolean isValidWidth(final int width) {
        return width == 1 || width == 2 || width == 4 || width == 8 || width == 16;
    }

    /**
     * Serializes this header to a new little-endian 64-byte buffer positioned at 0 with limit 64.
     */
    public ByteBuffer toBuffer() {
        final ByteBuffer b = ByteBuffer.allocate(SIZE).order(ByteOrder.LITTLE_ENDIAN);
        b.put(0, MAGIC);
        b.putInt(VERSION_OFFSET, Manifest.FORMAT_VERSION);
        b.put(BYTE_ORDER_OFFSET, LITTLE_ENDIAN);
        b.putInt(WIDTH_OFFSET, valueWidth);
        b.putLong(COUNT_OFFSET, count);
        b.putLong(CHECKSUM_OFFSET, checksum);
        return b;
    }

    /**
     * Parses and validates a header from {@code src}, starting at its current position and without changing it.
     *
     * @param src  a buffer with at least {@link #SIZE} bytes remaining
     * @param path the segment path, used only in error messages
     * @throws UncheckedIOException if the bytes are not a valid header
     */
    public static SegmentHeader parse(final ByteBuffer src, final Path path) {
        if (src.remaining() < SIZE) throw corrupt(path, "file is shorter than the " + SIZE + " byte header");
        final ByteBuffer b = src.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        final int base = b.position();
        for (int i = 0; i < MAGIC.length; i++) {
            if (b.get(base + i) != MAGIC[i]) throw corrupt(path, "bad magic");
        }
        final int version = b.getInt(base + VERSION_OFFSET);
        if (version != Manifest.FORMAT_VERSION) {
            throw corrupt(path, "unsupported format version " + version);
        }
        final byte order = b.get(base + BYTE_ORDER_OFFSET);
        if (order != LITTLE_ENDIAN) throw corrupt(path, "unsupported byte order " + order);
        final int width = b.getInt(base + WIDTH_OFFSET);
        if (!isValidWidth(width)) throw corrupt(path, "invalid value width " + width);
        final long count = b.getLong(base + COUNT_OFFSET);
        if (count < 0) throw corrupt(path, "negative value count " + count);
        final long checksum = b.getLong(base + CHECKSUM_OFFSET);
        return new SegmentHeader(width, count, checksum);
    }

    /**
     * Reads and validates the header of the segment at {@code path}, and checks that the file length matches it.
     *
     * @throws UncheckedIOException if the file cannot be read or is not a well-formed segment
     */
    public static SegmentHeader read(final Path path) {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            return read(ch, path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads and validates the header through an open channel, and checks that the file length matches it.
     *
     * @throws UncheckedIOException if the file cannot be read or is not a well-formed segment
     */
    public static SegmentHeader read(final FileChannel channel, final Path path) throws IOException {
        final ByteBuffer b = ByteBuffer.allocate(SIZE);
        int pos = 0;
        while (b.hasRemaining()) {
            final int n = channel.read(b, pos);
            if (n < 0) throw corrupt(path, "file is shorter than the " + SIZE + " byte header");
            pos += n;
        }
        b.flip();
        final SegmentHeader header = parse(b, path);
        final long expected;
        try {
            expected = header.fileBytes();
        } catch (ArithmeticException e) {
            throw corrupt(path, "payload length overflows");
        }
        final long actual = channel.size();
        if (actual != expected) {
            throw corrupt(path, "file length " + actual + " does not match header, expected " + expected);
        }
        return header;
    }

    /**
     * Computes the CRC32C of {@code payloadBytes} bytes starting at file offset {@link #SIZE}, reading through
     * positional reads so the channel's own position is untouched.
     */
    public static long computeChecksum(final FileChannel channel, final long payloadBytes) throws IOException {
        final CRC32C crc = new CRC32C();
        final ByteBuffer buf = ByteBuffer.allocateDirect(1 << 20);
        long pos = SIZE;
        long remaining = payloadBytes;
        while (remaining > 0) {
            buf.clear();
            if (remaining < buf.capacity()) buf.limit((int) remaining);
            final int n = channel.read(buf, pos);
            if (n < 0) throw new IOException("Unexpected end of file while computing checksum");
            buf.flip();
            crc.update(buf);
            pos += n;
            remaining -= n;
        }
        return crc.getValue();
    }

    static UncheckedIOException corrupt(final Path path, final String message) {
        return new UncheckedIOException(new IOException("Corrupt segment " + path + ": " + message));
    }

    @Override
    public String toString() {
        return "SegmentHeader{width=" + valueWidth + ", count=" + count + ", checksum=" + checksum + "}";
    }
}
