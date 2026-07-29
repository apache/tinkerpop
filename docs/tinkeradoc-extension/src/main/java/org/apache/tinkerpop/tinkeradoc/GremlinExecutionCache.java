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
package org.apache.tinkerpop.tinkeradoc;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A keyed store of executed {@link NeutralTab} groups, so live Gremlin runs exactly once even
 * though the document is rendered for multiple backends (HTML and Markdown).
 * <p>
 * AsciidoctorJ cannot re-target a loaded document's backend, so each backend is a separate
 * {@code convert()} that re-runs the treeprocessor. The first pass to reach a given Gremlin block
 * executes it and stores the resulting neutral tabs here; every later pass reads the cached tabs
 * and skips the console. Because both backends render from the <em>same</em> cached neutral data,
 * the executed {@code ==>} output is identical across HTML and Markdown by construction.
 * <p>
 * The production instance is {@link #SHARED}, backed by a concurrent map so it survives across the
 * separate Asciidoctor runtimes the asciidoctor-maven-plugin creates for each per-book
 * {@code <execution>} within one build JVM. Tests inject a private instance to avoid cross-test
 * leakage.
 */
class GremlinExecutionCache {

    /**
     * Process-wide cache used by the SPI-registered treeprocessor in production. Keys embed the
     * source document identity and the block's ordinal (see {@link #key}), so entries from
     * different books or positions never collide.
     */
    static final GremlinExecutionCache SHARED = new GremlinExecutionCache(new ConcurrentHashMap<>());

    private final Map<String, List<NeutralTab>> store;

    /** Creates a cache with its own private (non-shared) backing map, for tests. */
    GremlinExecutionCache() {
        this(new ConcurrentHashMap<>());
    }

    GremlinExecutionCache(final Map<String, List<NeutralTab>> store) {
        this.store = store;
    }

    /**
     * Builds a stable cache key for a Gremlin block. The key must be identical across the two
     * backend passes (which parse the same source in the same order) yet distinct across documents
     * and block positions, because a block's console output depends on the stateful console session
     * up to that point.
     *
     * @param documentId a stable identity for the source document (e.g. its {@code docfile})
     * @param ordinal    the block's 1-based position among Gremlin blocks in the document
     * @param source     the block's source text, hashed in as an integrity guard
     */
    static String key(final String documentId, final int ordinal, final String source) {
        return documentId + "#" + ordinal + "#" + (source == null ? 0 : source.hashCode());
    }

    boolean contains(final String key) {
        return store.containsKey(key);
    }

    List<NeutralTab> get(final String key) {
        return store.get(key);
    }

    void put(final String key, final List<NeutralTab> tabs) {
        store.put(key, tabs);
    }

    /** Removes all entries. Intended for test isolation. */
    void clear() {
        store.clear();
    }

    int size() {
        return store.size();
    }
}
