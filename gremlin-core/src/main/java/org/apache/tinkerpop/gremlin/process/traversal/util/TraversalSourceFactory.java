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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.Serializable;

/**
 * {@link TraversalSource} is not {@link Serializable}.
 * {@code TraversalSourceFactory} can be used to create a serializable representation of a traversal source.
 * This is is primarily an internal utility class for use and should not be used by standard users.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalSourceFactory<T extends TraversalSource> implements Serializable {

    private final TraversalStrategies traversalStrategies;
    private final Class<T> traversalSourceClass;

    public TraversalSourceFactory(final T traversalSource) {
        this.traversalSourceClass = (Class<T>) traversalSource.getClass();
        this.traversalStrategies = traversalSource.getStrategies();
    }

    public T createTraversalSource(final Graph graph) {
        try {
            return this.traversalSourceClass.getConstructor(Graph.class, TraversalStrategies.class).newInstance(graph, this.traversalStrategies);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
