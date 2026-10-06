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

import org.apache.tinkerpop.gremlin.structure.snapshot.spi.UnsupportedSnapshotDataException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes {@code id-index-keys.bin} and {@code id-index-ordinals.bin} from entries presented in
 * {@linkplain IdentifierIndex#compare index order}, that is sorted by key and then ordinal. The materializing builder
 * sorts in heap and calls {@link #addAllSorting(long[], int)}; the streaming builder external-sorts its spool and calls
 * {@link #add(long, int)} for every entry. The output is identical.
 * <p/>
 * The writer also enforces that identifiers are unique, failing the build with
 * {@link UnsupportedSnapshotDataException} on a duplicate. For an exact index a repeated key is a duplicate. For a
 * non-exact index a repeated key may merely be a hash collision, so the writer compares the colliding entries in the
 * identifier column, which must therefore be complete and mapped before the index is written.
 * <p/>
 * {@link #finish()} checks that the number of entries equals the element count, so no element is left out.
 * Instances are not thread-safe.
 */
public final class IdentifierIndexWriter implements AutoCloseable {

    private final SegmentWriter keys;
    private final SegmentWriter ordinals;
    private final String elementKind;
    private final long elementCount;
    private final ValueType exactType;
    private final ColumnReader ids;

    private boolean any;
    private long lastKey;
    private int lastOrdinal;
    // ordinals that share lastKey, for duplicate detection in a non-exact index
    private int[] group = new int[4];
    private int groupSize;
    private boolean finished;

    private IdentifierIndexWriter(final Path keysPath, final Path ordinalsPath, final String elementKind,
                                  final long elementCount, final ValueType exactType, final ColumnReader ids) {
        this.elementKind = elementKind;
        this.elementCount = elementCount;
        this.exactType = exactType;
        this.ids = ids;
        this.keys = SegmentWriter.create(keysPath, 8);
        try {
            this.ordinals = SegmentWriter.create(ordinalsPath, 4);
        } catch (RuntimeException e) {
            keys.close();
            throw e;
        }
    }

    /**
     * Creates an index writer.
     *
     * @param keysPath     where to write {@code id-index-keys.bin}
     * @param ordinalsPath where to write {@code id-index-ordinals.bin}
     * @param elementKind  {@code "vertex"} or {@code "edge"}, for error messages
     * @param elementCount the number of elements, which is the number of entries that must be added
     * @param exactType    the result of {@link IdentifierIndex#exactType(Manifest.ColumnInfo)} for the identifier
     *                     column, null for a non-exact index
     * @param ids          the complete identifier column, required unless the index is exact
     */
    public static IdentifierIndexWriter create(final Path keysPath, final Path ordinalsPath, final String elementKind,
                                               final long elementCount, final ValueType exactType,
                                               final ColumnReader ids) {
        if (exactType == null && ids == null) {
            throw new IllegalArgumentException("A non-exact identifier index needs the identifier column");
        }
        return new IdentifierIndexWriter(keysPath, ordinalsPath, elementKind, elementCount, exactType, ids);
    }

    /**
     * Whether the index being written is exact, which is the manifest's exact-index flag.
     */
    public boolean isExact() {
        return exactType != null;
    }

    /**
     * Appends the next entry.
     *
     * @throws IllegalStateException            if the entry is not after the previous one in index order
     * @throws UnsupportedSnapshotDataException if the identifier duplicates an earlier one
     */
    public void add(final long key, final int ordinal) {
        if (any) {
            final int order = IdentifierIndex.compare(lastKey, lastOrdinal, key, ordinal);
            if (order >= 0) {
                throw new IllegalStateException(elementKind + " identifier index entries out of order: (" + lastKey
                        + ", " + lastOrdinal + ") then (" + key + ", " + ordinal + ")");
            }
            if (key == lastKey) checkDuplicate(key, ordinal);
            else groupSize = 0;
        }
        if (groupSize == group.length) group = java.util.Arrays.copyOf(group, groupSize * 2);
        group[groupSize++] = ordinal;
        keys.writeLong(key);
        ordinals.writeInt(ordinal);
        any = true;
        lastKey = key;
        lastOrdinal = ordinal;
    }

    private void checkDuplicate(final long key, final int ordinal) {
        if (exactType != null) {
            throw duplicate(key, lastOrdinal, ordinal, null);
        }
        for (int i = 0; i < groupSize; i++) {
            if (ids.sameValue(group[i], ordinal)) {
                throw duplicate(key, group[i], ordinal, ids.get(ordinal));
            }
        }
    }

    private UnsupportedSnapshotDataException duplicate(final long key, final int first, final int second,
                                                       final Object id) {
        return new UnsupportedSnapshotDataException("Duplicate " + elementKind + " identifier "
                + (id != null ? id + " (" + id.getClass().getSimpleName() + ")" : "with key " + key)
                + " at ordinals " + first + " and " + second);
    }

    /**
     * Sorts the keys of all elements and adds every entry, for a builder that holds the keys in heap. The caller's array
     * is not modified.
     *
     * @param keysByOrdinal the key of each element, indexed by its ordinal
     * @param count         the number of elements
     */
    public void addAllSorting(final long[] keysByOrdinal, final int count) {
        final long[] sortedKeys = java.util.Arrays.copyOf(keysByOrdinal, count);
        final int[] sortedOrdinals = new int[count];
        for (int i = 0; i < count; i++) sortedOrdinals[i] = i;
        IdentifierIndex.sortEntries(sortedKeys, sortedOrdinals, count);
        for (int i = 0; i < count; i++) add(sortedKeys[i], sortedOrdinals[i]);
    }

    /**
     * The number of entries added so far.
     */
    public long count() {
        return keys.count();
    }

    /**
     * Completes both segments.
     *
     * @return the keys segment and then the ordinals segment, using the given bundle-relative paths
     * @throws IllegalStateException if the number of entries differs from the element count
     */
    public List<Manifest.SegmentInfo> finish(final String keysRelativePath, final String ordinalsRelativePath) {
        if (!finished) {
            if (keys.count() != elementCount) {
                throw new IllegalStateException(elementKind + " identifier index has " + keys.count()
                        + " entries for " + elementCount + " elements");
            }
            keys.finish();
            ordinals.finish();
            finished = true;
        }
        final List<Manifest.SegmentInfo> out = new ArrayList<>(2);
        out.add(keys.info(keysRelativePath));
        out.add(ordinals.info(ordinalsRelativePath));
        return out;
    }

    /**
     * Releases the files of an index that was not finished.
     */
    @Override
    public void close() {
        if (finished) return;
        try {
            keys.close();
        } finally {
            ordinals.close();
        }
    }
}
