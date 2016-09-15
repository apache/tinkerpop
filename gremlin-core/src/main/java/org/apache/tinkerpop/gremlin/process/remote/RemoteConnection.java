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
     * @deprecated As of release 3.2.2, replaced by {@link #submit(Bytecode)}.
     */
    @Deprecated
    public <E> Iterator<Traverser.Admin<E>> submit(final Traversal<?, E> traversal) throws RemoteConnectionException;

    /**
     * Submits {@link Traversal} {@link Bytecode} to a server and returns a {@link Traversal}.
     * The {@link Traversal} is an abstraction over two types of results that can be returned as part of the
     * response from the server: the results of the {@link Traversal} itself and the side-effects that it produced.
     */
    public <E> RemoteTraversal<?,E> submit(final Bytecode bytecode) throws RemoteConnectionException;
}
