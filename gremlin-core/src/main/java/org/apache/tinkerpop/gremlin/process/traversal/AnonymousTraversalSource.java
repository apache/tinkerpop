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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * Provides a unified way to construct a {@link TraversalSource} from the perspective of the traversal. In this syntax
 * the user is creating the source and binding it to a reference which is either an existing {@link Graph} instance
 * or a {@link RemoteConnection}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AnonymousTraversalSource<T extends TraversalSource> {

    private final Class<T> traversalSourceClass;

    private AnonymousTraversalSource(final Class<T> traversalSourceClass) {
        this.traversalSourceClass = traversalSourceClass;
    }

    /**
     * Constructs an {@code AnonymousTraversalSource} which will then be configured to spawn a
     * {@link GraphTraversalSource}.
     */
    public static AnonymousTraversalSource<GraphTraversalSource> traversal() {
        return traversal(GraphTraversalSource.class);
    }

    /**
     * Constructs an {@code AnonymousTraversalSource} which will then be configured to spawn the specified
     * {@link TraversalSource}.
     */
    public static <T extends TraversalSource> AnonymousTraversalSource<T> traversal(final Class<T> traversalSourceClass) {
        return new AnonymousTraversalSource<>(traversalSourceClass);
    }

    /**
     * Creates the specified {@link TraversalSource} binding a {@link RemoteConnection} as its reference such that
     * traversals spawned from it will execute over that reference.
     */
    public T withRemote(final String configFile) throws Exception {
        // for now, this method simply uses the deprecated approach. when this approach is removed we just need to
        // make it possible to set a RemoteConnection directly on TraversalSource construction.
        return (T) withGraph(EmptyGraph.instance()).withRemote(configFile);
    }

    /**
     * Creates the specified {@link TraversalSource} binding a {@link RemoteConnection} as its reference such that
     * traversals spawned from it will execute over that reference.
     */
    public T withRemote(final Configuration conf) {
        // for now, this method simply uses the deprecated approach. when this approach is removed we just need to
        // make it possible to set a RemoteConnection directly on TraversalSource construction.
        return (T) withGraph(EmptyGraph.instance()).withRemote(conf);
    }

    /**
     * Creates the specified {@link TraversalSource} binding a {@link RemoteConnection} as its reference such that
     * traversals spawned from it will execute over that reference.
     */
    public T withRemote(final RemoteConnection remoteConnection) {
        // for now, this method simply uses the deprecated approach. when this approach is removed we just need to
        // make it possible to set a RemoteConnection directly on TraversalSource construction.
        return (T) withGraph(EmptyGraph.instance()).withRemote(remoteConnection);
    }

    /**
     * Creates the specified {@link TraversalSource} binding a {@link Graph} as its reference such that traversals
     * spawned from it will execute over that reference.
     */
    public T withGraph(final Graph graph) {
        try {
            return traversalSourceClass.getConstructor(Graph.class).newInstance(graph);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
