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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Canonical registry of the toy graph datasets referenced by Gremlin documentation examples.
 * <p>
 * This is the single source of truth shared by two concerns that must never disagree about the set
 * of datasets:
 * <ul>
 *   <li><b>execution</b> — the Gremlin statement run to build the graph before an example
 *       ({@link GremlinTreeprocessor}); and</li>
 *   <li><b>rendering</b> — the dataset caption shown beneath an example and its link to the
 *       Sample Data book section ({@link TabbedHtmlBuilder}).</li>
 * </ul>
 * Adding a graph is therefore a one-line change here that feeds both, so a new dataset can never be
 * executable-but-unlabeled (or vice versa).
 */
final class GraphCatalog {

    /** A dataset entry: how to construct it, how to name it, and where its Sample Data book section lives. */
    static final class Entry {
        /** Gremlin statement that constructs the graph into the {@code graph} binding. */
        final String initStatement;
        /** Human-readable name used in the rendered dataset caption. */
        final String displayName;
        /** Sample Data book section anchor, or {@code null} if the graph has no dedicated section. */
        final String docAnchor;

        Entry(final String initStatement, final String displayName, final String docAnchor) {
            this.initStatement = initStatement;
            this.displayName = displayName;
            this.docAnchor = docAnchor;
        }
    }

    /** Init statement for a bare block or any unrecognized token: a fresh, empty graph. */
    static final String DEFAULT_INIT = "graph = TinkerGraph.open()";

    private static final Map<String, Entry> BY_TOKEN;

    static {
        final Map<String, Entry> m = new HashMap<>();
        // "crew" and "theCrew" are aliases for the same dataset.
        final Entry crew = new Entry("graph = TinkerFactory.createTheCrew()", "crew", "the-crew");
        m.put("modern", new Entry("graph = TinkerFactory.createModern()", "modern", "modern"));
        m.put("classic", new Entry("graph = TinkerFactory.createClassic()", "classic", null));
        m.put("crew", crew);
        m.put("theCrew", crew);
        m.put("grateful", new Entry("graph = TinkerFactory.createGratefulDead()", "Grateful Dead", "grateful-dead"));
        m.put("airroutes", new Entry("graph = TinkerFactory.createAirRoutes()", "Air Routes", "air-routes"));
        m.put("theZoo", new Entry("graph = TinkerFactory.createTheZoo()", "Zoo", "the-zoo"));
        m.put("sink", new Entry("graph = TinkerFactory.createKitchenSink()", "kitchen sink", null));
        BY_TOKEN = Collections.unmodifiableMap(m);
    }

    private GraphCatalog() {
    }

    /**
     * Returns the catalog entry for a dataset token, or {@code null} if the token is not a known
     * dataset (including {@code null}, a bare block, or a misspelling).
     */
    static Entry entry(final String token) {
        return token == null ? null : BY_TOKEN.get(token);
    }

    /**
     * Returns the Gremlin init statement for a token, falling back to {@link #DEFAULT_INIT} (a fresh
     * empty graph) for {@code null} or unrecognized tokens.
     */
    static String initStatement(final String token) {
        final Entry e = entry(token);
        return e != null ? e.initStatement : DEFAULT_INIT;
    }
}
