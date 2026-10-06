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
 * The value types a snapshot can store, each with a stable one-byte code and a canonical encoding. This enum is owned
 * by the snapshot format and deliberately does not reuse {@code GType} ordinals. Codes must never change or be reused.
 */
public enum ValueType {

    BOOLEAN(1, 1),
    BYTE(2, 1),
    SHORT(3, 2),
    CHAR(4, 2),
    INT(5, 4),
    FLOAT(6, 4),
    LONG(7, 8),
    DOUBLE(8, 8),
    STRING(9, ValueType.VARIABLE),
    UUID(10, ValueType.VARIABLE),
    BIGINT(11, ValueType.VARIABLE),
    BIGDECIMAL(12, ValueType.VARIABLE),
    DURATION(13, ValueType.VARIABLE),
    DATETIME(14, ValueType.VARIABLE),
    BINARY(15, ValueType.VARIABLE),
    LIST(16, ValueType.VARIABLE),
    SET(17, ValueType.VARIABLE),
    MAP(18, ValueType.VARIABLE),
    /**
     * Reserved for a null entry inside a collection. It has no payload and never appears outside a collection, so it is
     * never a column value type.
     */
    NULL(19, ValueType.VARIABLE);

    /**
     * The width reported by {@link #width()} for variable-width types.
     */
    public static final int VARIABLE = -1;

    private final byte code;
    private final int width;

    ValueType(final int code, final int width) {
        this.code = (byte) code;
        this.width = width;
    }

    /**
     * The stable one-byte code written to disk.
     */
    public byte code() {
        return code;
    }

    /**
     * The encoded width in bytes, or {@link #VARIABLE} for variable-width types.
     */
    public int width() {
        return width;
    }

    public boolean isFixedWidth() {
        return width != VARIABLE;
    }

    /**
     * Whether this type can be an identifier: every type except {@link #BINARY}, {@link #LIST}, {@link #SET},
     * {@link #MAP} and {@link #NULL}.
     */
    public boolean isScalar() {
        return code <= DATETIME.code;
    }

    /**
     * Whether this type is a {@link #LIST}, {@link #SET} or {@link #MAP}.
     */
    public boolean isCollection() {
        return this == LIST || this == SET || this == MAP;
    }

    /**
     * Resolves a type from its on-disk code.
     *
     * @throws IllegalArgumentException if the code is unknown
     */
    public static ValueType fromCode(final byte code) {
        for (final ValueType t : values()) {
            if (t.code == code) return t;
        }
        throw new IllegalArgumentException("Unknown value type code: " + code);
    }
}
