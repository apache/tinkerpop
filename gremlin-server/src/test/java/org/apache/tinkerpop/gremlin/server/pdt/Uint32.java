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
package org.apache.tinkerpop.gremlin.server.pdt;

import java.util.Objects;

/**
 * A test fixture representing an unsigned 32-bit integer, backed by a {@code long}.
 * Used to exercise PrimitivePDT numeric round-trip in integration tests.
 */
public final class Uint32 {

    private final long value;

    public Uint32(final long value) {
        if (value < 0 || value > 4294967295L)
            throw new IllegalArgumentException("Uint32 value out of range: " + value);
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof Uint32)) return false;
        return value == ((Uint32) o).value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return "Uint32(" + value + ")";
    }
}
