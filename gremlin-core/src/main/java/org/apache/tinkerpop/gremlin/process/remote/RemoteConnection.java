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

import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

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

    /**
     * @deprecated As of release 3.2.2, replaced by {@link #submitAsync(Bytecode)}.
     */
    @Deprecated
    public <E> Iterator<Traverser.Admin<E>> submit(final Traversal<?, E> traversal) throws RemoteConnectionException;

    /**
     * Submits {@link Traversal} {@link Bytecode} to a server and returns a {@link RemoteTraversal}.
     * The {@link RemoteTraversal} is an abstraction over two types of results that can be returned as part of the
     * response from the server: the results of the {@link Traversal} itself and the side-effects that it produced.
     *
     * @deprecated As of release 3.2.4, replaced by {@link #submitAsync(Bytecode)}.
     */
    @Deprecated
    public <E> RemoteTraversal<?,E> submit(final Bytecode bytecode) throws RemoteConnectionException;

    /**
     * Submits {@link Traversal} {@link Bytecode} to a server and returns a promise of a {@link RemoteTraversal}.
     * The {@link RemoteTraversal} is an abstraction over two types of results that can be returned as part of the
     * response from the server: the results of the {@link Traversal} itself and the side-effects that it produced.
     * <p/>
     * The default implementation calls the {@link #submit(Bytecode)} method for backward compatibility, but generally
     * speaking this method should be implemented directly as {@link #submit(Bytecode)} is not called directly by
     * any part of TinkerPop. Even if the {@code RemoteConnection} itself is not capable of asynchronous behaviour, it
     * should simply implement this method in a blocking form.
     */
    public default <E> CompletableFuture<RemoteTraversal<?, E>> submitAsync(final Bytecode bytecode) throws RemoteConnectionException {
        // default implementation for backward compatibility to 3.2.4 - this method will probably just become
        // the new submit() in 3.3.x when the deprecation is removed
        final CompletableFuture<RemoteTraversal<?, E>> promise = new CompletableFuture<>();
        try {
            promise.complete(submit(bytecode));
        } catch (Exception t) {
            promise.completeExceptionally(t);
        }
        return promise;
    }
}
