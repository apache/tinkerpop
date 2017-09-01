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

import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.remote.traversal.EmbeddedRemoteTraversal;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Iterator;

/**
 * Allows a {@link RemoteConnection} to be submitted to a "local" {@link Graph} instance thus simulating a connection
 * to a remote source. Basic usage is as follows:
 *
 * <pre>
 * {@code
 * // Using TinkerGraph here but any embedded Graph instance would suffice
 * Graph graph = TinkerFactory.createModern();
 * GraphTraversalSource g = graph.traversal();
 *
 * // setup the remote as normal but give it the embedded "g" so that it executes against that
 * final Graph remote = EmptyGraph.instance();
 * GraphTraversalSource simulatedRemoteG = remote.traversal().withRemote(new EmbeddedRemoteConnection(g));
 * assertEquals(6, simulatedRemoteG.V().count().next().intValue());
 * }
 * </pre>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EmbeddedRemoteConnection implements RemoteConnection {

    private final GraphTraversalSource g;

    public EmbeddedRemoteConnection(final GraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public <E> Iterator<Traverser.Admin<E>> submit(final Traversal<?, E> traversal) throws RemoteConnectionException {
        throw new UnsupportedOperationException("This method is deprecated and no longer used internally");
    }

    @Override
    public <E> RemoteTraversal<?, E> submit(final Bytecode bytecode) throws RemoteConnectionException {
        // this method is technically deprecated but it's still being used on the 3.2.x/3.3.x lines of code. it is
        // called by submitAsync() in RemoteConnection itself. this implementation should suffice for now
        return new EmbeddedRemoteTraversal(JavaTranslator.of(g).translate(bytecode));
    }

    @Override
    public void close() throws Exception {
         g.close();
    }
}
