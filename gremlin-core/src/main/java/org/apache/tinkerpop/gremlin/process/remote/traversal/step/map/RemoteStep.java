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
package org.apache.tinkerpop.gremlin.process.remote.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sends a {@link Traversal} to a {@link RemoteConnection} and iterates back the results.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class RemoteStep<S, E> extends AbstractStep<S, E> {

    private transient RemoteConnection remoteConnection;
    private RemoteTraversal<?, E> remoteTraversal;
    private final AtomicReference<CompletableFuture<Traversal<?, E>>> traversalFuture = new AtomicReference<>(null);

    public RemoteStep(final Traversal.Admin traversal, final RemoteConnection remoteConnection) {
        super(traversal);
        this.remoteConnection = remoteConnection;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.remoteConnection);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (null == this.remoteTraversal) {
            try {
                promise().join();
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                // If the underlying future failed, join() will throw a CompletionException, for consistency
                // with previous behavior:
                // - Throw underlying exception if it was unchecked (RuntimeException or Error).
                // - Wrap in IllegalStateException otherwise.
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else if (cause instanceof Error) {
                    throw (Error) cause;
                }
                throw new IllegalStateException(cause);
            }
        }
        return this.remoteTraversal.nextTraverser();
    }

    /**
     * Submits the traversal asynchronously to a "remote" using {@link RemoteConnection#submitAsync(GremlinLang)}.
     */
    public CompletableFuture<Traversal<?, E>> promise() {
        try {
            if (null == traversalFuture.get()) {
                traversalFuture.set(this.remoteConnection.submitAsync(this.traversal.getGremlinLang()).<Traversal<?, E>>thenApply(t -> {
                    this.remoteTraversal = (RemoteTraversal<?, E>) t;
                    return traversal;
                }));
            }

            return traversalFuture.get();
        } catch (RemoteConnectionException rce) {
            throw new IllegalStateException(rce);
        }
    }
}
