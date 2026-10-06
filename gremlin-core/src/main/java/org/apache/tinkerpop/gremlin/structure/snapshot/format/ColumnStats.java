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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Statistics gathered for one column during a scan: how many elements have an entry, how many of those entries are
 * null, and which value types were observed. Once the scan is complete and the element count is known, {@link #toInfo(long)} applies the encoding
 * rules shared by both builders:
 * <ul>
 *     <li>a column is dense when {@code presentCount * 4 >= elementCount} and sparse otherwise;</li>
 *     <li>a column of exactly one fixed-width type uses the fixed layout;</li>
 *     <li>every other column, that is a single variable-width type, mixed types, or no values at all, uses the
 *     variable layout, and a mixed column additionally has a {@code types.bin} segment.</li>
 * </ul>
 * Call {@link #add(ValueType)} once per present non-null value and {@link #addNull()} once per present null value. A
 * null is an entry that counts towards the present count, so it takes a position in the column, but it adds no value
 * type: a column whose only entries are nulls has no types and uses the variable layout, which costs an empty range
 * per entry. The null count is recorded in the {@link Manifest.ColumnInfo} and decides whether the column has a
 * {@code nulls.bin}. Instances are not thread-safe.
 */
public final class ColumnStats {

    private long presentCount;
    private long nullCount;
    private int typeMask;
    // integral range, tracked only for values added with their value
    private long rangedCount;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;

    /**
     * Records one present value of the given type.
     */
    public void add(final ValueType type) {
        if (type == ValueType.NULL) throw new IllegalArgumentException("Use addNull() for null values");
        presentCount++;
        typeMask |= 1 << type.code();
    }

    /**
     * Records one present value of the given type and, when the type is integral, folds the value into the range
     * reported by {@link Manifest.ColumnInfo#minValue()} and {@link Manifest.ColumnInfo#maxValue()}. A column
     * reports a range only if every non-null value was added through this method.
     *
     * @param value the non-null value, whose class is the one {@link ValueCodec#requireType} mapped to {@code type}
     */
    public void add(final ValueType type, final Object value) {
        add(type);
        if (ValueCodec.isIntegral(type)) {
            final long v = ((Number) value).longValue();
            rangedCount++;
            if (v < min) min = v;
            if (v > max) max = v;
        }
    }

    /**
     * Records one present null value. It counts as present but adds no value type.
     */
    public void addNull() {
        presentCount++;
        nullCount++;
    }

    /**
     * The number of null values recorded so far, which is also counted by {@link #presentCount()}.
     */
    public long nullCount() {
        return nullCount;
    }

    /**
     * The number of present entries recorded so far, including nulls.
     */
    public long presentCount() {
        return presentCount;
    }

    /**
     * The observed value types in ascending {@link ValueType#code()} order.
     */
    public List<ValueType> types() {
        final List<ValueType> out = new ArrayList<>();
        for (int code = 1; code < Integer.SIZE; code++) {
            if ((typeMask & (1 << code)) != 0) out.add(ValueCodec.typeOfCode((byte) code));
        }
        return Collections.unmodifiableList(out);
    }

    /**
     * Whether a column with the given counts is dense, that is {@code presentCount * 4 >= elementCount}.
     */
    public static boolean isDense(final long elementCount, final long presentCount) {
        return presentCount * 4 >= elementCount;
    }

    /**
     * The encoding chosen for the given counts and observed types.
     */
    public static ColumnEncoding chooseEncoding(final long elementCount, final long presentCount,
                                                final List<ValueType> types) {
        final boolean dense = isDense(elementCount, presentCount);
        final boolean fixed = types.size() == 1 && types.get(0).isFixedWidth();
        if (fixed) return dense ? ColumnEncoding.DENSE_FIXED : ColumnEncoding.SPARSE_FIXED;
        return dense ? ColumnEncoding.DENSE_VARIABLE : ColumnEncoding.SPARSE_VARIABLE;
    }

    /**
     * The manifest description of the column these statistics describe.
     *
     * @param elementCount the number of elements of the column's kind: all vertices, or all edges
     * @throws IllegalStateException if more values were recorded than there are elements
     */
    public Manifest.ColumnInfo toInfo(final long elementCount) {
        if (presentCount > elementCount) {
            throw new IllegalStateException("Column has " + presentCount + " values for " + elementCount + " elements");
        }
        final List<ValueType> types = types();
        Long minValue = null;
        Long maxValue = null;
        if (!types.isEmpty() && rangedCount == presentCount - nullCount) {
            boolean integral = true;
            for (final ValueType t : types) integral &= ValueCodec.isIntegral(t);
            if (integral) {
                minValue = min;
                maxValue = max;
            }
        }
        return new Manifest.ColumnInfo(chooseEncoding(elementCount, presentCount, types), types, presentCount, nullCount,
                minValue, maxValue);
    }
}
