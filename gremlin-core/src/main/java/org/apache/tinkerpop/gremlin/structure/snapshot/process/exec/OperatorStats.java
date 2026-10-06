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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The counters of one operator, maintained by {@link AbstractCsrOperator} and published by {@code CsrSuperStep} as one
 * nested metrics entry per operator when the traversal is profiled. Time is exclusive: it excludes the time spent
 * pulling from upstream.
 */
public final class OperatorStats {

    private long entriesIn;
    private long entriesOut;
    private long bulkOut;
    private long batches;
    private long nanos;
    private final Map<String, Object> annotations = new LinkedHashMap<>();

    void addIn(final long entries) {
        entriesIn += entries;
    }

    void addOut(final long entries, final long bulk) {
        entriesOut += entries;
        bulkOut += bulk;
        batches++;
    }

    void addNanos(final long nanos) {
        this.nanos += nanos;
    }

    /**
     * Entries pulled from upstream.
     */
    public long entriesIn() {
        return entriesIn;
    }

    /**
     * Entries emitted, which is the traverser count of a profile.
     */
    public long entriesOut() {
        return entriesOut;
    }

    /**
     * The summed bulk of the emitted entries, which is the element count of a profile.
     */
    public long bulkOut() {
        return bulkOut;
    }

    /**
     * The number of non-empty batches emitted.
     */
    public long batches() {
        return batches;
    }

    public long nanos() {
        return nanos;
    }

    /**
     * Sets a profile annotation such as the frontier representation, push or pull, bytes reserved or spills.
     *
     * @param value a {@code String} or a {@code Number}
     */
    public void annotate(final String key, final Object value) {
        if (!(value instanceof String) && !(value instanceof Number)) {
            throw new IllegalArgumentException("Profile annotations hold Strings and Numbers only");
        }
        annotations.put(key, value);
    }

    public Map<String, Object> annotations() {
        return Collections.unmodifiableMap(annotations);
    }

    void clear() {
        entriesIn = 0;
        entriesOut = 0;
        bulkOut = 0;
        batches = 0;
        nanos = 0;
        annotations.clear();
    }
}
