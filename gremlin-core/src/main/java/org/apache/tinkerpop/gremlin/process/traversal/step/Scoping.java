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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.Map;
import java.util.Set;

/**
 * This interface is implemented by {@link Step} implementations that access labeled path steps, side-effects or
 * {@code Map} values by key, such as {@code select('a')} step. Note that a step like {@code project()} is non-scoping
 * because while it creates a {@code Map} it does not introspect them.
 * <p/>
 * There are four types of scopes:
 * <ol>
 *   <li>Current scope</li> — the current data referenced by the traverser (“path head”).
 *   <li>Path scope</li> — a particular piece of data in the path of the traverser (“path history”).
 *   <li>Side-effect scope</li> — a particular piece of data in the global traversal blackboard.
 *   <li>Map scope</li> — a particular piece of data in the current scope map (“map value by key”).
 * </ol>
 *
 * The current scope refers to the current object referenced by the traverser. That is, the {@code traverser.get()}
 * object. Another way to think about the current scope is to think in terms of the path of the traverser where the
 * current scope is the head of the path. With the math()-step, the variable {@code _} refers to the current scope.
 *
 * <pre>
 * {@code
 * gremlin> g.V().values("age").math("sin _")
 * ==>-0.6636338842129675
 * ==>0.956375928404503
 * ==>0.5514266812416906
 * ==>-0.428182669496151
 * }
 * </pre>
 *
 * The path scope refers to data previously seen by the traverser. That is, data in the traverser’s path history.
 * Paths can be accessed by {@code path()}, however, individual parts of the path can be labeled using {@code as()}
 * and accessed later via the path label name. Thus, in the traversal below, “a” and “b” refer to objects previously
 * traversed by the traverser.
 *
 * <pre>
 * {@code
 * gremlin> g.V().as("a").out("knows").as("b”).
 * math("a / b").by("age")
 * ==>1.0740740740740742
 * ==>0.90625
 * }
 * </pre>
 *
 * The side-effect scope refers objects in the global side-effects of the traversal. Side-effects are not local to the
 * traverser, but instead, global to the traversal. In the traversal below you can see how “x” is being referenced in
 * the math()-step and thus, the side-effect data is being used.
 *
 * <pre>
 * {@code
 * gremlin> g.withSideEffect("x",100).V().values("age").math("_ / x")
 * ==>0.29
 * ==>0.27
 * ==>0.32
 * ==>0.35
 * }
 * </pre>
 *
 * Map scope refers to objects within the current map object. Thus, its like current scope, but a bit “deeper.” In the
 * traversal below the {@code project()}-step generates a map with keys “a” and “b”. The subsequent {@code math()}-step
 * is then able to access the “a” and “b” values in the respective map and use them for the division operation.
 *
 * <pre>
 * {@code gremlin>
 * g.V().hasLabel("person”).
 * project("a","b”).
 *   by("age”).
 *   by(bothE().count()).
 * math("a / b")
 * ==>9.666666666666666
 * ==>27.0
 * ==>10.666666666666666
 * ==>35.0
 * }
 * </pre>
 *
 * Scoping is all about variable data access and forms the fundamental interface for access to the memory structures
 * of Gremlin.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Scoping {

    public enum Variable {START, END}

    public default <S> S getScopeValue(final Pop pop, final String key, final Traverser.Admin<?> traverser) throws IllegalArgumentException {
        if (traverser.getSideEffects().exists(key))
            return traverser.getSideEffects().get(key);
        ///
        final Object object = traverser.get();
        if (object instanceof Map && ((Map<String, S>) object).containsKey(key))
            return ((Map<String, S>) object).get(key);
        ///
        final Path path = traverser.path();
        if (path.hasLabel(key))
            return null == pop ? path.get(key) : path.get(pop, key);
        ///
        throw new IllegalArgumentException("Neither the sideEffects, map, nor path has a " + key + "-key: " + this);
    }

    public default <S> S getNullableScopeValue(final Pop pop, final String key, final Traverser.Admin<?> traverser) {
        if (traverser.getSideEffects().exists(key))
            return traverser.getSideEffects().get(key);
        ///
        final Object object = traverser.get();
        if (object instanceof Map && ((Map<String, S>) object).containsKey(key))
            return ((Map<String, S>) object).get(key);
        ///
        final Path path = traverser.path();
        if (path.hasLabel(key))
            return null == pop ? path.get(key) : path.get(pop, key);
        ///
        return null;
    }

    /**
     * Get the labels that this scoping step will access during the traversal
     *
     * @return the accessed labels of the scoping step
     */
    public Set<String> getScopeKeys();
}
