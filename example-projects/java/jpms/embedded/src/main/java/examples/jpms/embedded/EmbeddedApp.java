/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package examples.jpms.embedded;

import org.apache.tinkerpop.gremlin.jsr223.DefaultGremlinScriptEngineManager;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRank;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import javax.script.Bindings;
import java.lang.reflect.InaccessibleObjectException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;

/**
 * Runs TinkerGraph embedded in a named module: OLTP traversals over the "modern" graph, the {@code gremlin-lang}
 * script engine (discovered through {@code ServiceLoader}), and an OLAP job on {@link TinkerGraphComputer}. It also
 * checks that Gryo, which is unsupported on the module path, fails as documented. Any failed check throws, so the
 * process exits with a non-zero status.
 */
public final class EmbeddedApp {

    private EmbeddedApp() {}

    public static void main(final String[] args) throws Exception {
        try (TinkerGraph graph = TinkerFactory.createModern()) {
            final GraphTraversalSource g = traversal().with(graph);
            runTraversals(g);
            runScriptEngine(g);
            runGraphComputer(g);
        }
        checkGryoIsUnsupported();
        System.out.println("embedded example completed");
    }

    private static void runTraversals(final GraphTraversalSource g) {
        check(6L, g.V().count().next(), "vertex count");

        final Set<Object> friends = new HashSet<>(g.V().has("person", "name", "marko").out("knows").values("name").toList());
        check(new HashSet<>(Arrays.asList("vadas", "josh")), friends, "people marko knows");

        final Map<Object, Long> byLabel = g.V().groupCount().by(T.label).next();
        check(4L, byLabel.get("person"), "person count");
        check(2L, byLabel.get("software"), "software count");

        // math() is backed by exp4j, an automatic module that gremlin-core requires
        final List<Double> agesPlusOne = g.V().has("person", "name", "marko").values("age").math("_ + 1").toList();
        check(List.of(30.0d), agesPlusOne, "math() result");

        final Set<Object> twoHops = new HashSet<>(g.V().repeat(out()).times(2).values("name").toList());
        check(new HashSet<>(Arrays.asList("ripple", "lop")), twoHops, "repeat() results");
    }

    private static void runScriptEngine(final GraphTraversalSource g) throws Exception {
        // the engine is found through the GremlinScriptEngineFactory service that gremlin-core provides
        final GremlinScriptEngine engine = new DefaultGremlinScriptEngineManager().getEngineByName("gremlin-lang");
        if (null == engine)
            throw new IllegalStateException("the gremlin-lang script engine was not discovered");

        final Bindings bindings = engine.createBindings();
        bindings.put("g", g);
        final Object result = engine.eval("g.V().hasLabel('software').count()", bindings);
        check(2L, ((Traversal<?, ?>) result).next(), "gremlin-lang result");
    }

    private static void runGraphComputer(final GraphTraversalSource g) {
        final List<Object> ranks = g.withComputer(TinkerGraphComputer.class).V().
                pageRank().with(PageRank.propertyName, "rank").values("rank").toList();
        check(6, ranks.size(), "number of pageRank() results");
        for (final Object rank : ranks) {
            if (!(rank instanceof Double) || (Double) rank <= 0.0d)
                throw new IllegalStateException("unexpected pageRank() value: " + rank);
        }
    }

    /**
     * Gryo relies on Kryo reflecting into JDK and TinkerPop classes, which named modules don't open to it, so the
     * Gryo-backed sample graphs can't be loaded on the module path.
     */
    private static void checkGryoIsUnsupported() {
        try {
            TinkerFactory.createGratefulDead().close();
        } catch (Throwable t) {
            for (Throwable cause = t; cause != null; cause = cause.getCause()) {
                if (cause instanceof InaccessibleObjectException) {
                    System.out.println("Gryo is unsupported on the module path, as expected: " + cause);
                    return;
                }
            }
            throw new IllegalStateException("Gryo failed, but not because of module encapsulation", t);
        }
        throw new IllegalStateException("Gryo was expected to fail on the module path but loaded the Grateful Dead graph");
    }

    private static void check(final Object expected, final Object actual, final String what) {
        if (!expected.equals(actual))
            throw new IllegalStateException(what + ": expected " + expected + " but was " + actual);
    }
}
