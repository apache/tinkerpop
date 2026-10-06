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
 * A growable big-endian byte sink for spill records.
 */
final class Bytes {

    private byte[] a = new byte[64];
    private int n;

    void reset() {
        n = 0;
    }

    int size() {
        return n;
    }

    private void ensure(final int extra) {
        if (n + extra > a.length) a = Arrays.copyOf(a, Math.max(a.length * 2, n + extra));
    }

    void putByte(final int v) {
        ensure(1);
        a[n++] = (byte) v;
    }

    void putInt(final int v) {
        ensure(4);
        a[n++] = (byte) (v >>> 24);
        a[n++] = (byte) (v >>> 16);
        a[n++] = (byte) (v >>> 8);
        a[n++] = (byte) v;
    }

    void putLong(final long v) {
        putInt((int) (v >>> 32));
        putInt((int) v);
    }

    void putBytes(final byte[] src, final int off, final int len) {
        ensure(len);
        System.arraycopy(src, off, a, n, len);
        n += len;
    }

    void putBytes(final byte[] src) {
        putBytes(src, 0, src.length);
    }

    void putBytes(final Bytes other) {
        putBytes(other.a, 0, other.n);
    }

    /**
     * A length-prefixed block.
     */
    void putBlock(final byte[] src) {
        putInt(src.length);
        putBytes(src, 0, src.length);
    }

    void putString(final String s) {
        putBlock(s.getBytes(StandardCharsets.UTF_8));
    }

    byte[] toArray() {
        return Arrays.copyOf(a, n);
    }
}
