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
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * A memory-mapped segment with random access by value index, in read-only or read-write mode.
 * <p/>
 * The file is mapped in fixed regions of {@link #regionBytes()} bytes, counted from file offset 0 so that the header
 * is part of region 0. The region size is a power of two of at least {@link SegmentHeader#SIZE}, and so is a
 * multiple of every value width, as is the header size. A fixed-width value therefore never crosses a region
 * boundary, and segments larger than 2 GiB need no special handling. Only {@link #getBytes} and {@link #putBytes},
 * meant for the width-1 payload of variable-width columns, copy across region boundaries.
 * <p/>
 * Typed accessors take a value index in {@code [0, count())} and must match the segment's value width. They throw
 * {@link IndexOutOfBoundsException} for a bad index and {@link IllegalStateException} for a width mismatch. Writes
 * on a read-only segment throw {@link java.nio.ReadOnlyBufferException}.
 * <p/>
 * A read-write segment created with {@link #create} is zero-filled, has the count in its header, and has a zero
 * checksum until {@link #finish()} computes the real one. A segment that was created or reopened read-write and never
 * finished is therefore detectable through {@link #verifyChecksum()}. Java offers no portable way to unmap a buffer:
 * {@link #close()} flushes a read-write segment and releases the references, and the operating system reclaims the
 * mapping when the buffers are garbage collected. The file may be deleted or renamed in the meantime on POSIX
 * systems. Accessors must not be called after {@link #close()}.
 * <p/>
 * Instances may be read from several threads; concurrent writes to distinct values are safe, as for any
 * {@link MappedByteBuffer}, provided the callers do not write the same value concurrently.
 */
public final class MappedSegment implements AutoCloseable {

    /**
     * The default region size, 1 GiB.
     */
    public static final long DEFAULT_REGION_BYTES = 1L << 30;

    /**
     * The largest supported region size, 1 GiB, so that a region fits a {@link ByteBuffer}.
     */
    public static final long MAX_REGION_BYTES = 1L << 30;

    public enum Mode {
        READ_ONLY, READ_WRITE
    }

    private final Path path;
    private final Mode mode;
    private final int valueWidth;
    private final long count;
    private final long regionBytes;
    private final int regionShift;
    private final long regionMask;
    private final MappedByteBuffer[] regions;
    private long checksum;

    private MappedSegment(final Path path, final Mode mode, final SegmentHeader header, final long regionBytes,
                          final FileChannel channel) throws IOException {
        this.path = path;
        this.mode = mode;
        this.valueWidth = header.valueWidth();
        this.count = header.count();
        this.checksum = header.checksum();
        this.regionBytes = regionBytes;
        this.regionShift = Long.numberOfTrailingZeros(regionBytes);
        this.regionMask = regionBytes - 1;
        final long fileBytes = header.fileBytes();
        final int regionCount = (int) ((fileBytes + regionBytes - 1) >>> regionShift);
        this.regions = new MappedByteBuffer[regionCount];
        final FileChannel.MapMode mapMode = mode == Mode.READ_ONLY ? FileChannel.MapMode.READ_ONLY
                : FileChannel.MapMode.READ_WRITE;
        for (int i = 0; i < regionCount; i++) {
            final long start = (long) i << regionShift;
            final long size = Math.min(regionBytes, fileBytes - start);
            regions[i] = channel.map(mapMode, start, size);
            regions[i].order(ByteOrder.LITTLE_ENDIAN);
        }
    }

    /**
     * Maps an existing segment with the {@linkplain #DEFAULT_REGION_BYTES default region size}. The header and the file
     * length are validated.
     *
     * @throws UncheckedIOException if the file cannot be mapped or is not a well-formed segment
     */
    public static MappedSegment open(final Path path, final Mode mode) {
        return open(path, mode, DEFAULT_REGION_BYTES);
    }

    /**
     * Maps an existing segment with an explicit region size, so small regions can be exercised.
     *
     * @param regionBytes a power of two in {@code [SegmentHeader.SIZE, MAX_REGION_BYTES]}
     * @throws UncheckedIOException if the file cannot be mapped or is not a well-formed segment
     */
    public static MappedSegment open(final Path path, final Mode mode, final long regionBytes) {
        Objects.requireNonNull(mode);
        checkRegionBytes(regionBytes);
        final StandardOpenOption[] options = mode == Mode.READ_ONLY
                ? new StandardOpenOption[]{StandardOpenOption.READ}
                : new StandardOpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE};
        try (FileChannel channel = FileChannel.open(path, options)) {
            final SegmentHeader header = SegmentHeader.read(channel, path);
            return new MappedSegment(path, mode, header, regionBytes, channel);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Creates a zero-filled read-write segment of {@code count} values, replacing any existing file and creating
     * missing parent directories, with the {@linkplain #DEFAULT_REGION_BYTES default region size}. The file is
     * sparse where the filesystem supports it. Call {@link #finish()} when all values have been written.
     *
     * @param valueWidth the value width in bytes: 1, 2, 4, 8 or 16
     */
    public static MappedSegment create(final Path path, final int valueWidth, final long count) {
        return create(path, valueWidth, count, DEFAULT_REGION_BYTES);
    }

    /**
     * As {@link #create(Path, int, long)} with an explicit region size.
     *
     * @param regionBytes a power of two in {@code [SegmentHeader.SIZE, MAX_REGION_BYTES]}
     */
    public static MappedSegment create(final Path path, final int valueWidth, final long count, final long regionBytes) {
        checkRegionBytes(regionBytes);
        final SegmentHeader header = new SegmentHeader(valueWidth, count, 0);
        try {
            final Path parent = path.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            Files.deleteIfExists(path);
            try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) {
                file.setLength(header.fileBytes());
            }
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                final ByteBuffer h = header.toBuffer();
                while (h.hasRemaining()) channel.write(h, h.position());
                return new MappedSegment(path, Mode.READ_WRITE, header, regionBytes, channel);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void checkRegionBytes(final long regionBytes) {
        if (regionBytes < SegmentHeader.SIZE || regionBytes > MAX_REGION_BYTES
                || Long.bitCount(regionBytes) != 1) {
            throw new IllegalArgumentException("Region size must be a power of two in [" + SegmentHeader.SIZE
                    + ", " + MAX_REGION_BYTES + "]: " + regionBytes);
        }
    }

    public Path path() {
        return path;
    }

    public Mode mode() {
        return mode;
    }

    public int valueWidth() {
        return valueWidth;
    }

    /**
     * The number of values in the segment.
     */
    public long count() {
        return count;
    }

    public long regionBytes() {
        return regionBytes;
    }

    /**
     * The number of mapped regions, including the one holding the header.
     */
    public int regionCount() {
        return regions.length;
    }

    /**
     * The header checksum as of the last {@link #finish()}, or as read when the segment was opened.
     */
    public long checksum() {
        return checksum;
    }

    /**
     * Describes the segment for the manifest, using the current {@link #checksum()}.
     *
     * @param relativePath the path relative to the bundle root, using {@code '/'} separators
     */
    public Manifest.SegmentInfo info(final String relativePath) {
        return new Manifest.SegmentInfo(relativePath, valueWidth, count, checksum);
    }

    public byte getByte(final long index) {
        final long off = offset(index, 1);
        return regions[(int) (off >>> regionShift)].get((int) (off & regionMask));
    }

    public short getShort(final long index) {
        final long off = offset(index, 2);
        return regions[(int) (off >>> regionShift)].getShort((int) (off & regionMask));
    }

    public int getInt(final long index) {
        final long off = offset(index, 4);
        return regions[(int) (off >>> regionShift)].getInt((int) (off & regionMask));
    }

    public long getLong(final long index) {
        final long off = offset(index, 8);
        return regions[(int) (off >>> regionShift)].getLong((int) (off & regionMask));
    }

    public void putByte(final long index, final byte value) {
        final long off = offset(index, 1);
        regions[(int) (off >>> regionShift)].put((int) (off & regionMask), value);
    }

    public void putShort(final long index, final short value) {
        final long off = offset(index, 2);
        regions[(int) (off >>> regionShift)].putShort((int) (off & regionMask), value);
    }

    public void putInt(final long index, final int value) {
        final long off = offset(index, 4);
        regions[(int) (off >>> regionShift)].putInt((int) (off & regionMask), value);
    }

    public void putLong(final long index, final long value) {
        final long off = offset(index, 8);
        regions[(int) (off >>> regionShift)].putLong((int) (off & regionMask), value);
    }

    /**
     * Copies {@code length} values of a width-1 segment, starting at {@code startIndex}, into {@code dst}. The range
     * may span region boundaries.
     */
    public void getBytes(final long startIndex, final byte[] dst, final int offset, final int length) {
        copy(startIndex, dst, offset, length, false);
    }

    /**
     * Copies {@code length} bytes from {@code src} into a width-1 segment starting at {@code startIndex}. The range
     * may span region boundaries.
     */
    public void putBytes(final long startIndex, final byte[] src, final int offset, final int length) {
        copy(startIndex, src, offset, length, true);
    }

    /**
     * Recomputes the CRC32C of the payload, stores it in the header, and flushes the segment to disk. Call after all
     * values are written. Read-write mode only.
     *
     * @throws UnsupportedOperationException on a read-only segment
     */
    public void finish() {
        if (mode != Mode.READ_WRITE) throw new UnsupportedOperationException("Segment is read-only: " + path);
        force();
        checksum = computeChecksum();
        final SegmentHeader header = new SegmentHeader(valueWidth, count, checksum);
        final ByteBuffer h = header.toBuffer();
        final MappedByteBuffer first = regions[0];
        for (int i = 0; i < SegmentHeader.SIZE; i++) first.put(i, h.get(i));
        force();
    }

    /**
     * Flushes modified regions to disk. A no-op in read-only mode.
     */
    public void force() {
        if (mode == Mode.READ_WRITE) {
            for (final MappedByteBuffer region : regions) region.force();
        }
    }

    /**
     * Computes the CRC32C of the payload from the mapped data.
     */
    public long computeChecksum() {
        final CRC32C crc = new CRC32C();
        for (int i = 0; i < regions.length; i++) {
            final ByteBuffer dup = regions[i].duplicate();
            dup.clear();
            if (i == 0) dup.position(SegmentHeader.SIZE);
            crc.update(dup);
        }
        return crc.getValue();
    }

    /**
     * Compares {@link #computeChecksum()} with the header checksum.
     *
     * @throws UncheckedIOException if they differ
     */
    public void verifyChecksum() {
        final long actual = computeChecksum();
        if (actual != checksum) {
            throw SegmentHeader.corrupt(path, "checksum mismatch, header " + checksum + " computed " + actual);
        }
    }

    /**
     * Flushes a read-write segment. The mapping is released when the buffers are garbage collected.
     */
    @Override
    public void close() {
        force();
    }

    // byte offset in the file of the value at index, after checking width and bounds
    private long offset(final long index, final int width) {
        if (valueWidth != width) {
            throw new IllegalStateException("Segment " + path + " has value width " + valueWidth + ", not " + width);
        }
        Objects.checkIndex(index, count);
        return SegmentHeader.SIZE + index * width;
    }

    private void copy(final long startIndex, final byte[] array, final int arrayOffset, final int length,
                      final boolean write) {
        if (valueWidth != 1) {
            throw new IllegalStateException("Segment " + path + " has value width " + valueWidth + ", not 1");
        }
        Objects.checkFromIndexSize(arrayOffset, length, array.length);
        Objects.checkFromIndexSize(startIndex, length, count);
        long off = SegmentHeader.SIZE + startIndex;
        int arrayPos = arrayOffset;
        int remaining = length;
        while (remaining > 0) {
            final ByteBuffer region = regions[(int) (off >>> regionShift)];
            final int inRegion = (int) (off & regionMask);
            final int n = (int) Math.min(remaining, region.limit() - inRegion);
            final ByteBuffer view = region.duplicate();
            view.position(inRegion);
            if (write) {
                view.put(array, arrayPos, n);
            } else {
                view.get(array, arrayPos, n);
            }
            off += n;
            arrayPos += n;
            remaining -= n;
        }
    }
}
