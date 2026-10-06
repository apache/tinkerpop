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

import java.util.Arrays;

/**
 * Accumulates the number of elements per label code during a scan, for the optional label count statistics of the
 * {@link Manifest}. Both builders use it so they record identical counts. A label code counts once for each occurrence,
 * so a multi-label vertex counts once for each of its labels. Instances are not thread-safe.
 */
public final class LabelCounts {

    private long[] counts = new long[8];

    /**
     * Records one occurrence of the label with the given code.
     */
    public void increment(final int code) {
        if (code >= counts.length) counts = Arrays.copyOf(counts, Math.max(code + 1, counts.length * 2));
        counts[code]++;
    }

    /**
     * The counts indexed by label code.
     *
     * @param labelCount the size of the label dictionary, which is the length of the result
     */
    public long[] toArray(final int labelCount) {
        return Arrays.copyOf(counts, labelCount);
    }
}
