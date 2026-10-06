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

import org.apache.tinkerpop.gremlin.structure.snapshot.format.IdentifierIndex;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Sorts the entries of an identifier-index spool into {@linkplain IdentifierIndex#compare index order} with bounded
 * heap. The input is a width-8 segment of keys in which the position of a key is its ordinal. Chunks that fit the
 * memory budget are sorted in heap and written to scratch as runs of {@code (key, ordinal)} pairs, and the runs are
 * merged k-way. When there are more runs than the merge fan-in, which the budget bounds, intermediate passes merge
 * groups of runs into longer ones first. An input that fits a single chunk is sorted and emitted without any run files.
 */
final class ExternalIndexSorter {

    @FunctionalInterface
    interface EntrySink {
        void accept(long key, int ordinal);
    }

    // heap per entry while sorting a chunk: key and ordinal, and the copy that the stable sort needs
    private static final int BYTES_PER_CHUNK_ENTRY = 24;
    private static final int MAX_CHUNK_ENTRIES = 1 << 26;
    private static final int MAX_FAN_IN = 1024;

    private final Function<String, Path> scratch;
    private final int chunkEntries;
    private final int bufferBytes;
    private final int fanIn;
    private int runSerial;

    /**
     * @param scratch     maps a scratch-relative path to a file, creating missing parent directories
     * @param budgetBytes the memory the sort may use: half for a sort chunk and half for merge buffers
     */
    ExternalIndexSorter(final Function<String, Path> scratch, final long budgetBytes) {
        this.scratch = scratch;
        this.chunkEntries = (int) Math.max(16, Math.min(MAX_CHUNK_ENTRIES, budgetBytes / 2 / BYTES_PER_CHUNK_ENTRY));
        this.bufferBytes = (int) Math.max(64, Math.min(SegmentReader.DEFAULT_BUFFER_BYTES, budgetBytes / 128));
        this.fanIn = (int) Math.max(2, Math.min(MAX_FAN_IN, budgetBytes / 2 / bufferBytes));
    }

    /**
     * Feeds every entry of the spool to the sink in index order.
     *
     * @param keysSpool a finished width-8 segment of keys; the ordinal of a key is its position
     * @param name      distinguishes the run files of different sorts
     */
    void sort(final Path keysSpool, final String name, final EntrySink sink) {
        List<Path> runs = new ArrayList<>();
        try {
            try (SegmentReader in = SegmentReader.open(keysSpool, bufferBytes)) {
                final long n = in.count();
                if (n <= chunkEntries) {
                    final int size = (int) n;
                    final long[] keys = new long[size];
                    final int[] ordinals = new int[size];
                    for (int i = 0; i < size; i++) {
                        keys[i] = in.readLong();
                        ordinals[i] = i;
                    }
                    IdentifierIndex.sortEntries(keys, ordinals, size);
                    for (int i = 0; i < size; i++) sink.accept(keys[i], ordinals[i]);
                    return;
                }
                final long[] keys = new long[chunkEntries];
                final int[] ordinals = new int[chunkEntries];
                long base = 0;
                while (base < n) {
                    final int size = (int) Math.min(chunkEntries, n - base);
                    for (int i = 0; i < size; i++) {
                        keys[i] = in.readLong();
                        ordinals[i] = (int) (base + i);
                    }
                    IdentifierIndex.sortEntries(keys, ordinals, size);
                    final Path run = newRun(name);
                    runs.add(run);
                    try (SegmentWriter out = SegmentWriter.create(run, 8, bufferBytes)) {
                        for (int i = 0; i < size; i++) {
                            out.writeLong(keys[i]);
                            out.writeLong(ordinals[i]);
                        }
                    }
                    base += size;
                }
            }

            while (runs.size() > fanIn) {
                final List<Path> next = new ArrayList<>();
                for (int from = 0; from < runs.size(); from += fanIn) {
                    final List<Path> group = runs.subList(from, Math.min(from + fanIn, runs.size()));
                    if (group.size() == 1) {
                        next.add(group.get(0));
                        continue;
                    }
                    final Path merged = newRun(name);
                    next.add(merged);
                    try (SegmentWriter out = SegmentWriter.create(merged, 8, bufferBytes)) {
                        merge(group, (key, ordinal) -> {
                            out.writeLong(key);
                            out.writeLong(ordinal);
                        });
                    }
                    for (final Path p : group) delete(p);
                }
                runs = next;
            }
            merge(runs, sink);
        } finally {
            for (final Path p : runs) delete(p);
        }
    }

    private Path newRun(final String name) {
        return scratch.apply("sort/" + name + "-run-" + (runSerial++) + ".bin");
    }

    private static void delete(final Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * A sequential cursor over one run.
     */
    private static final class Cursor implements AutoCloseable {
        private final SegmentReader reader;
        private long key;
        private int ordinal;

        Cursor(final Path path, final int bufferBytes) {
            this.reader = SegmentReader.open(path, bufferBytes);
        }

        boolean advance() {
            if (!reader.hasRemaining()) return false;
            key = reader.readLong();
            ordinal = (int) reader.readLong();
            return true;
        }

        boolean before(final Cursor other) {
            return IdentifierIndex.compare(key, ordinal, other.key, other.ordinal) < 0;
        }

        @Override
        public void close() {
            reader.close();
        }
    }

    private void merge(final List<Path> runs, final EntrySink sink) {
        final List<Cursor> cursors = new ArrayList<>(runs.size());
        try {
            final Cursor[] heap = new Cursor[runs.size()];
            int size = 0;
            for (final Path run : runs) {
                final Cursor cursor = new Cursor(run, bufferBytes);
                cursors.add(cursor);
                if (cursor.advance()) heap[size++] = cursor;
            }
            for (int i = size / 2 - 1; i >= 0; i--) siftDown(heap, i, size);
            while (size > 0) {
                final Cursor top = heap[0];
                sink.accept(top.key, top.ordinal);
                if (top.advance()) {
                    siftDown(heap, 0, size);
                } else {
                    heap[0] = heap[--size];
                    heap[size] = null;
                    if (size > 0) siftDown(heap, 0, size);
                }
            }
        } finally {
            for (final Cursor cursor : cursors) {
                try {
                    cursor.close();
                } catch (RuntimeException ignored) {
                    // the build is failing or the run is about to be deleted
                }
            }
        }
    }

    private static void siftDown(final Cursor[] heap, final int start, final int size) {
        int parent = start;
        final Cursor moving = heap[parent];
        while (true) {
            int child = 2 * parent + 1;
            if (child >= size) break;
            if (child + 1 < size && heap[child + 1].before(heap[child])) child++;
            if (!heap[child].before(moving)) break;
            heap[parent] = heap[child];
            parent = child;
        }
        heap[parent] = moving;
    }
}
