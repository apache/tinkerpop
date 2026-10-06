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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

import org.apache.tinkerpop.gremlin.structure.snapshot.format.MappedSegment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A temporary directory of read-write {@link MappedSegment}s for spilling operator state, deleted on {@link #close()}.
 * The directory is created on the first {@link #create} call, so an execution that never spills touches no disk.
 * Segments are fixed-size, zero-filled and written by index; an operator that spills a stream of unknown length creates
 * one segment per run or chunk, and finishes it with {@link MappedSegment#finish()} if it needs a checksum. The
 * helper counts what was created for profile annotations. Instances are not thread-safe.
 */
public final class ScratchSpace implements AutoCloseable {

    private final Path base;
    private Path directory;
    private final List<MappedSegment> segments = new ArrayList<>();
    private long counter;
    private long segmentsCreated;
    private long bytesCreated;

    /**
     * @param base the directory to create the scratch directory in, or null for the system temporary directory
     */
    public ScratchSpace(final Path base) {
        this.base = base;
    }

    /**
     * Creates a zero-filled read-write segment of {@code count} values of {@code valueWidth} bytes.
     *
     * @param name a hint for the file name, for example the operator and run number
     */
    public MappedSegment create(final String name, final int valueWidth, final long count) {
        final Path path = directory().resolve(counter++ + "-" + name.replaceAll("[^A-Za-z0-9._-]", "_") + ".bin");
        final MappedSegment segment = MappedSegment.create(path, valueWidth, count);
        segments.add(segment);
        segmentsCreated++;
        bytesCreated += count * valueWidth;
        return segment;
    }

    /**
     * Closes a segment and deletes its file, for example once a merged run is consumed. Does nothing for a segment this
     * space did not create.
     */
    public void discard(final MappedSegment segment) {
        if (!segments.remove(segment)) return;
        segment.close();
        try {
            Files.deleteIfExists(segment.path());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The number of segments created so far.
     */
    public long segmentsCreated() {
        return segmentsCreated;
    }

    /**
     * The payload bytes of the segments created so far.
     */
    public long bytesCreated() {
        return bytesCreated;
    }

    /**
     * The scratch directory, or null if nothing has been created yet.
     */
    public Path directoryOrNull() {
        return directory;
    }

    private Path directory() {
        if (directory == null) {
            try {
                directory = base == null ? Files.createTempDirectory("csr-scratch-")
                        : Files.createDirectories(base).resolve("csr-scratch-" + UUID.randomUUID());
                if (base != null) Files.createDirectories(directory);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return directory;
    }

    /**
     * Closes every segment and deletes the directory with its files.
     */
    @Override
    public void close() {
        for (final MappedSegment s : segments) s.close();
        segments.clear();
        if (directory == null) return;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
            for (final Path file : files) Files.deleteIfExists(file);
            Files.deleteIfExists(directory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            directory = null;
        }
    }
}
