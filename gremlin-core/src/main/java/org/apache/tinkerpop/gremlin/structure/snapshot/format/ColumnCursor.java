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

/**
 * Looks up the entries of a {@link ColumnReader} for ascending element ordinals. A sparse column is advanced
 * sequentially through {@code ordinals.bin}, so scanning a whole batch costs one pass instead of a binary search per
 * ordinal; a dense column answers directly. An ordinal below the previous one is still answered correctly, by a binary
 * search that repositions the cursor. Instances are stateful and not thread-safe; use one per scan.
 */
public final class ColumnCursor {

    private final ColumnReader reader;
    // sparse columns: the entry index of the first element not below the last requested ordinal
    private long position;
    private long lastOrdinal = -1;

    ColumnCursor(final ColumnReader reader) {
        this.reader = reader;
    }

    public ColumnReader reader() {
        return reader;
    }

    /**
     * The entry index of the element, or -1 when it has no value; see {@link ColumnReader#entryIndex(long)}.
     */
    public long seek(final long ordinal) {
        if (reader.isDense()) return reader.entryIndex(ordinal);
        if (ordinal < lastOrdinal) {
            final long entry = reader.entryIndex(ordinal);
            lastOrdinal = ordinal;
            position = entry >= 0 ? entry : lowerBound(ordinal);
            return entry;
        }
        lastOrdinal = ordinal;
        final long count = reader.entryCount();
        while (position < count && reader.ordinalAt(position) < ordinal) position++;
        return position < count && reader.ordinalAt(position) == ordinal ? position : -1;
    }

    // the entry index of the first element whose ordinal is not below the given one
    private long lowerBound(final long ordinal) {
        long lo = 0;
        long hi = reader.entryCount();
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (reader.ordinalAt(mid) < ordinal) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /**
     * Whether the element has a value, see {@link ColumnReader#isPresent(long)}.
     */
    public boolean isPresent(final long ordinal) {
        return seek(ordinal) >= 0;
    }
}
