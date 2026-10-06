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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A reader over a byte array written by {@link Bytes}.
 */
final class ByteSource {

    private final byte[] a;
    private int pos;

    ByteSource(final byte[] a) {
        this(a, 0);
    }

    ByteSource(final byte[] a, final int pos) {
        this.a = a;
        this.pos = pos;
    }

    int position() {
        return pos;
    }

    boolean hasRemaining() {
        return pos < a.length;
    }

    byte getByte() {
        return a[pos++];
    }

    int getInt() {
        final int v = ((a[pos] & 0xFF) << 24) | ((a[pos + 1] & 0xFF) << 16) | ((a[pos + 2] & 0xFF) << 8)
                | (a[pos + 3] & 0xFF);
        pos += 4;
        return v;
    }

    long getLong() {
        final long high = getInt();
        return (high << 32) | (getInt() & 0xFFFFFFFFL);
    }

    byte[] getBytes(final int len) {
        final byte[] b = Arrays.copyOfRange(a, pos, pos + len);
        pos += len;
        return b;
    }

    byte[] getBlock() {
        return getBytes(getInt());
    }

    String getString() {
        return new String(getBlock(), StandardCharsets.UTF_8);
    }
}
