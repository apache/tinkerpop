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

import java.util.Arrays;

/**
 * The canonical encoding of a key as a hash map key, with the bytes that rebuild the representative when the canonical
 * form alone cannot.
 */
final class KeyBytes {

    static final byte[] EMPTY = new byte[0];

    final byte[] canon;
    final byte[] rep;
    private final int hash;

    KeyBytes(final byte[] canon, final byte[] rep) {
        this.canon = canon;
        this.rep = rep;
        this.hash = Arrays.hashCode(canon);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof KeyBytes && Arrays.equals(canon, ((KeyBytes) other).canon);
    }

    /**
     * The hash of canonical bytes for partitioning; the seed changes with the recursion depth so that a partition that
     * is too large splits differently.
     */
    static int partitionHash(final byte[] a, final int off, final int len, final int seed) {
        int h = 0x9E3779B9 * (seed + 1);
        for (int i = off; i < off + len; i++) h = 31 * h + a[i];
        h ^= h >>> 16;
        h *= 0x85EBCA6B;
        h ^= h >>> 13;
        h *= 0xC2B2AE35;
        h ^= h >>> 16;
        return h;
    }
}
