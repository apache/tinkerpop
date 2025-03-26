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
package org.apache.tinkerpop.gremlin.process.remote;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Transaction;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * A simple abstraction of a "connection" to a "server" that is capable of processing a {@link Traversal} and
 * returning results. Results refer to both the {@link Iterator} of results from the submitted {@link Traversal}
 * as well as the side-effects produced by that {@link Traversal}. Those results together are wrapped in a
 * {@link Traversal}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface RemoteConnection extends AutoCloseable {
    public static final String GREMLIN_REMOTE = "gremlin.remote.";
    public static final String GREMLIN_REMOTE_CONNECTION_CLASS = GREMLIN_REMOTE + "remoteConnectionClass";

    /**
     * Creates a {@link Transaction} object designed to work with remote semantics.
     */
    public default Transaction tx() {
        throw new UnsupportedOperationException("This implementation does not support remote transactions");
    }

    /**
     * Submits {@link Traversal} {@link GremlinLang} to a server and returns a promise of a {@link RemoteTraversal}.
     * The {@link RemoteTraversal} is an abstraction over two types of results that can be returned as part of the
     * response from the server: the results of the {@link Traversal} itself and the side-effects that it produced.
     */
    public <E> CompletableFuture<RemoteTraversal<?, E>> submitAsync(final GremlinLang gremlinLang) throws RemoteConnectionException;

    /**
     * Create a {@link RemoteConnection} from a {@code Configuration} object. The configuration must contain a
     * {@code gremlin.remote.remoteConnectionClass} key which is the fully qualified class name of a
     * {@link RemoteConnection} class.
     */
    public static RemoteConnection from(final Configuration conf) {
        if (!conf.containsKey(RemoteConnection.GREMLIN_REMOTE_CONNECTION_CLASS))
            throw new IllegalArgumentException("Configuration must contain the '" + GREMLIN_REMOTE_CONNECTION_CLASS + "' key");

        try {
            final Class<? extends RemoteConnection> clazz = Class.forName(
                    conf.getString(GREMLIN_REMOTE_CONNECTION_CLASS)).asSubclass(RemoteConnection.class);
            final Constructor<? extends RemoteConnection> ctor = clazz.getConstructor(Configuration.class);
            return ctor.newInstance(conf);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
