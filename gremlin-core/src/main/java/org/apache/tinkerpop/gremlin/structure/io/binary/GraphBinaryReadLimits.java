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
package org.apache.tinkerpop.gremlin.structure.io.binary;

import java.util.Objects;

/**
 * The bounds a {@link GraphBinaryReader} applies to the length and count prefixes it reads from the wire before
 * using them to size an allocation. The values are exposed as the {@code maxGraphBinaryValueLength},
 * {@code maxGraphBinaryInitialCapacity} and {@code maxGraphBinaryBulkedListLength} settings of Gremlin Server and the Java driver. A limit may be raised up to
 * {@link Integer#MAX_VALUE}.
 */
public final class GraphBinaryReadLimits {

    /**
     * The default for {@link #getMaxValueLength()}, which is 64MB.
     */
    public static final int DEFAULT_MAX_VALUE_LENGTH = 64 * 1024 * 1024;

    /**
     * The default for {@link #getMaxInitialCapacity()}, which is 16,384.
     */
    public static final int DEFAULT_MAX_INITIAL_CAPACITY = 16384;

    /**
     * The default for {@link #getMaxBulkedListLength()}, which is 16,777,216.
     */
    public static final int DEFAULT_MAX_BULKED_LIST_LENGTH = 1 << 24;

    /**
     * The limits applied when none are configured.
     */
    public static final GraphBinaryReadLimits DEFAULT = new GraphBinaryReadLimits(
            DEFAULT_MAX_VALUE_LENGTH, DEFAULT_MAX_INITIAL_CAPACITY, DEFAULT_MAX_BULKED_LIST_LENGTH);

    private final int maxValueLength;
    private final int maxInitialCapacity;
    private final int maxBulkedListLength;

    private GraphBinaryReadLimits(final int maxValueLength, final int maxInitialCapacity,
                                  final int maxBulkedListLength) {
        this.maxValueLength = maxValueLength;
        this.maxInitialCapacity = maxInitialCapacity;
        this.maxBulkedListLength = maxBulkedListLength;
    }

    /**
     * Creates limits from the supplied values, each of which must be greater than zero.
     *
     * @throws IllegalArgumentException if a value is not greater than zero
     */
    public static GraphBinaryReadLimits of(final int maxValueLength, final int maxInitialCapacity,
                                           final int maxBulkedListLength) {
        if (maxValueLength < 1)
            throw new IllegalArgumentException("maxGraphBinaryValueLength must be greater than zero");


        if (maxInitialCapacity < 1)
            throw new IllegalArgumentException("maxGraphBinaryInitialCapacity must be greater than zero");

        if (maxBulkedListLength < 1)
            throw new IllegalArgumentException("maxGraphBinaryBulkedListLength must be greater than zero");

        return new GraphBinaryReadLimits(maxValueLength, maxInitialCapacity, maxBulkedListLength);
    }

    /**
     * The maximum length in bytes of a single {@code String}, {@code BigInteger} or binary value. A value declaring
     * a longer length is rejected.
     */
    public int getMaxValueLength() {
        return maxValueLength;
    }

    /**
     * The maximum capacity reserved up front for a collection or map read from the wire. This does not limit the
     * size of the collection, which grows as elements are read. It only bounds the memory reserved before they are.
     */
    public int getMaxInitialCapacity() {
        return maxInitialCapacity;
    }

    /**
     * The maximum number of entries that a single bulked list may expand to. A bulked list sends each distinct item
     * once with a repeat count and is expanded into a full list when read, so a small message can otherwise describe
     * a very large list. A bulked list that would expand beyond this size is rejected.
     */
    public int getMaxBulkedListLength() {
        return maxBulkedListLength;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GraphBinaryReadLimits that = (GraphBinaryReadLimits) o;
        return maxValueLength == that.maxValueLength
                && maxInitialCapacity == that.maxInitialCapacity
                && maxBulkedListLength == that.maxBulkedListLength;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxValueLength, maxInitialCapacity, maxBulkedListLength);
    }

    @Override
    public String toString() {
        return "GraphBinaryReadLimits{maxValueLength=" + maxValueLength
                + ", maxInitialCapacity=" + maxInitialCapacity + ", maxBulkedListLength=" + maxBulkedListLength + "}";
    }
}
