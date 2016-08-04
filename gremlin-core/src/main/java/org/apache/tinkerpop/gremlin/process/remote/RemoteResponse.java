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

import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;

import java.util.Iterator;

/**
 * The {@code RemoteResponse} is returned from {@link RemoteConnection#submit(Bytecode)} and provides implementers a
 * way to represent how they will return the results of a submitted {@link Traversal} and its side-effects. The
 * {@code RemoteResponse} is used internally by traversals spawned from a {@link RemoteGraph} to put remote results
 * into the streams of those traversals and to replace client-side {@link TraversalSideEffects} in those traversals.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface RemoteResponse<E> {

    /**
     * Gets the list of results from a {@link Traversal} executed remotely. Implementers may push their results into
     * a {@link RemoteTraverser} instance to feed that {@code Iterator} or create their own implementation of it if
     * there is some advantage to doing so.
     */
    public Iterator<Traverser.Admin<E>> getTraversers();

    /**
     * Gets the side-effects (if any) from the remotely executed {@link Traversal}. Simple implementations could
     * likely use {@link DefaultTraversalSideEffects}, but more advanced implementations might look to lazily load
     * side-effects or otherwise implement some form of blocking to ensure that all side-effects are present from the
     * remote location.
     */
    public TraversalSideEffects getSideEffects();
}
