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
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

/**
 * The durability mode a {@link TinkerStorage} engine applies when flushing a committed transaction to disk, selected
 * with the {@code gremlin.tinkergraph.storage.sync} configuration key. Each value is a complete, mutually-exclusive
 * choice; they are ordered from strongest to weakest durability.
 * <p/>
 * The name of a mode describes <em>when</em> data is made durable, not <em>what</em> action is taken: {@link #COMMIT}
 * performs an {@code fsync} so acknowledged commits survive an OS crash or power loss, whereas {@link #OS} only pushes
 * bytes into the operating system's page cache, so commits survive a crash of the JVM process but not of the OS.
 */
public enum SyncMode {

    /**
     * {@code fsync} on every commit. An acknowledged commit is durable against process crash, OS crash, and power
     * loss. This is the default and the mode that honors the "each committed transaction is durably written to disk"
     * contract.
     */
    COMMIT,

    /**
     * Flush to the operating system on every commit, but do not {@code fsync}. An acknowledged commit survives a crash
     * of the JVM process but may be lost on an OS crash or power loss, since the data can still be sitting in the OS
     * page cache. Faster than {@link #COMMIT}; use only when that weaker guarantee is acceptable.
     */
    OS;

    // TODO: add an INTERVAL mode (group commit) — a peer value on this same key, encoded as "interval:<ms>", that
    // fsyncs at most once per <ms> window rather than once per commit, bounding the crash-loss window by time while
    // amortizing fsync cost across commits. It implies fsync (a batched COMMIT), so it slots in as a third
    // mutually-exclusive mode without changing the meaning of COMMIT or OS. Deferred until concurrent commits are
    // serialized, since group commit's payoff is amortizing one fsync across many concurrent committers.

    /**
     * Resolve a configuration value to a {@link SyncMode}, matched case-insensitively, defaulting to {@link #COMMIT}
     * when unset.
     *
     * @param configValue the raw configuration value, or {@code null} when unset
     * @return the resolved mode
     * @throws IllegalArgumentException if the value does not name a known mode
     */
    public static SyncMode fromConfigValue(final String configValue) {
        if (null == configValue)
            return COMMIT;
        try {
            return SyncMode.valueOf(configValue.trim().toUpperCase());
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException(String.format(
                    "Unknown storage sync mode '%s'; valid values are 'commit' and 'os'", configValue), iae);
        }
    }
}
