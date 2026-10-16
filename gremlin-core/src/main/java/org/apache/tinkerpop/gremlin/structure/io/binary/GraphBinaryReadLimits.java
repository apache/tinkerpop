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
 * {@code maxGraphBinaryArrayLength} and {@code maxGraphBinaryInitialCapacity} settings of Gremlin Server and the
 * Java driver. A limit may be raised up to {@link Integer#MAX_VALUE}.
 */
public final class GraphBinaryReadLimits {

    /**
     * The default for {@link #getMaxValueLength()}, which is 64MB.
     */
    public static final int DEFAULT_MAX_VALUE_LENGTH = 64 * 1024 * 1024;

    /**
     * The default for {@link #getMaxArrayLength()}, which is 1,048,576.
     */
    public static final int DEFAULT_MAX_ARRAY_LENGTH = 1 << 20;

    /**
     * The default for {@link #getMaxInitialCapacity()}, which is 16,384.
     */
    public static final int DEFAULT_MAX_INITIAL_CAPACITY = 16384;

    /**
     * The limits applied when none are configured.
     */
    public static final GraphBinaryReadLimits DEFAULT = new GraphBinaryReadLimits(
            DEFAULT_MAX_VALUE_LENGTH, DEFAULT_MAX_ARRAY_LENGTH, DEFAULT_MAX_INITIAL_CAPACITY);

    private final int maxValueLength;
    private final int maxArrayLength;
    private final int maxInitialCapacity;

    private GraphBinaryReadLimits(final int maxValueLength, final int maxArrayLength, final int maxInitialCapacity) {
        this.maxValueLength = maxValueLength;
        this.maxArrayLength = maxArrayLength;
        this.maxInitialCapacity = maxInitialCapacity;
    }

    /**
     * Creates limits from the supplied values, each of which must be greater than zero.
     *
     * @throws IllegalArgumentException if a value is not greater than zero
     */
    public static GraphBinaryReadLimits of(final int maxValueLength, final int maxArrayLength, final int maxInitialCapacity) {
        if (maxValueLength < 1)
            throw new IllegalArgumentException("maxGraphBinaryValueLength must be greater than zero");

        if (maxArrayLength < 1)
            throw new IllegalArgumentException("maxGraphBinaryArrayLength must be greater than zero");

        if (maxInitialCapacity < 1)
            throw new IllegalArgumentException("maxGraphBinaryInitialCapacity must be greater than zero");

        return new GraphBinaryReadLimits(maxValueLength, maxArrayLength, maxInitialCapacity);
    }

    /**
     * The maximum length in bytes of a single {@code String}, {@code BigInteger} or binary value. A value declaring
     * a longer length is rejected.
     */
    public int getMaxValueLength() {
        return maxValueLength;
    }

    /**
     * The maximum number of elements in a fixed-size array read from the wire, such as the arguments of a predicate.
     * An array declaring a larger count is rejected.
     */
    public int getMaxArrayLength() {
        return maxArrayLength;
    }

    /**
     * The maximum capacity reserved up front for a collection or map read from the wire. This does not limit the
     * size of the collection, which grows as elements are read. It only bounds the memory reserved before they are.
     */
    public int getMaxInitialCapacity() {
        return maxInitialCapacity;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GraphBinaryReadLimits that = (GraphBinaryReadLimits) o;
        return maxValueLength == that.maxValueLength
                && maxArrayLength == that.maxArrayLength
                && maxInitialCapacity == that.maxInitialCapacity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxValueLength, maxArrayLength, maxInitialCapacity);
    }

    @Override
    public String toString() {
        return "GraphBinaryReadLimits{maxValueLength=" + maxValueLength + ", maxArrayLength=" + maxArrayLength
                + ", maxInitialCapacity=" + maxInitialCapacity + "}";
    }
}
