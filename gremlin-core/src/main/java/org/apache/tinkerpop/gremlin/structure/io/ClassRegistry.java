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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the classes a GraphBinary, GraphSON or Gryo {@code Class} value may name, so such a name is resolved from
 * this registry rather than by a class loader. Registration is a Java API only and refuses a
 * {@link TraversalStrategy}, which belongs to {@link TraversalStrategies.GlobalCache}.
 * <p/>
 * {@link #lookup(String)} also falls back to {@code GlobalCache.getRegisteredStrategyClassByFullName}, so a strategy
 * the selector can construct is nameable without being registered here.
 */
public final class ClassRegistry {

    private ClassRegistry() {
    }

    /**
     * Fully qualified names ({@link Class#getName()}) of the classes a serialized traversal may name as a generic
     * {@code Class} value but not construct. {@link #lookup(String)} guards a {@code null} name itself, because a
     * {@code ConcurrentHashMap} refuses a {@code null} key.
     */
    private static final Map<String, Class<?>> REGISTRY = new ConcurrentHashMap<>();

    // this block writes REGISTRY through register(), so the field above has to be declared first.
    static {
        // Gryo names these two out of MatchAlgorithmStrategy's matchAlgorithmClass field, the count one being what that
        // strategy's builder holds when no algorithm is named.
        register(MatchStep.GreedyMatchAlgorithm.class);
        register(MatchStep.CountMatchAlgorithm.class);

        // Gryo names these two out of HaltedTraverserStrategy's haltedTraverserFactory field, which holds one or the
        // other and nothing else.
        register(DetachedFactory.class);
        register(ReferenceFactory.class);
    }

    /**
     * Registers a class as nameable as a generic {@code Class} value, without making it constructible from a serialized
     * traversal, so it cannot widen what the selector can construct. A {@link TraversalStrategy} is refused with an
     * {@link IllegalArgumentException} and a {@code null} with a {@link NullPointerException}.
     */
    public static void register(final Class<?> clazz) {
        Objects.requireNonNull(clazz, "clazz can not be null");
        rejectStrategy(clazz);

        REGISTRY.put(clazz.getName(), clazz);
    }

    /**
     * Removes a class from this registry, so it is no longer nameable as a generic {@code Class} value. A
     * {@link TraversalStrategy} is refused because strategies belong to {@link TraversalStrategies.GlobalCache}, and a
     * {@code null} is refused with a {@link NullPointerException}.
     */
    public static void unregister(final Class<?> clazz) {
        Objects.requireNonNull(clazz, "clazz can not be null");
        rejectStrategy(clazz);

        REGISTRY.remove(clazz.getName());
    }

    /**
     * Looks up a class nameable as a generic {@code Class} value, without loading it. Matching is on the exact
     * {@link Class#getName()}, never assignability, so registering a superclass or an interface does not admit its
     * subtypes. A {@code null} name yields an empty {@link Optional}, since a {@code Class} value can name nothing.
     */
    public static Optional<Class<?>> lookup(final String className) {
        // a null key would throw, and it can match nothing, so it leaves before the fall-back and its initialization.
        if (null == className)
            return Optional.empty();

        final Class<?> clazz = REGISTRY.get(className);

        // this call also forces GlobalCache initialization, which registers the strategies from registerStrategies().
        // Leave it here: a cached copy of what it reads would not.
        return null != clazz
                ? Optional.of(clazz)
                : TraversalStrategies.GlobalCache.getRegisteredStrategyClassByFullName(className)
                        .map(c -> (Class<?>) c);
    }

    /**
     * Refuses a {@link TraversalStrategy}, so that a caller who reaches for this registry is sent to the one the
     * strategy selector reads and {@link #lookup(String)} falls back to. Both callers guard {@code null} before this
     * runs, so a class is assumed here.
     */
    private static void rejectStrategy(final Class<?> clazz) {
        if (TraversalStrategy.class.isAssignableFrom(clazz))
            throw new IllegalArgumentException("TraversalStrategy classes are registered with " +
                    "TraversalStrategies.GlobalCache, not ClassRegistry - " + clazz.getName());
    }
}
