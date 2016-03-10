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
package org.apache.tinkerpop.gremlin.process.server.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.server.ServerConnection;
import org.apache.tinkerpop.gremlin.process.server.ServerConnectionException;
import org.apache.tinkerpop.gremlin.process.server.traversal.strategy.ServerStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Takes a {@link Traversal}
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ServerResultStep<S,E> extends AbstractStep<S, E> implements TraversalParent {
    private transient ServerConnection serverConnection;
    private PureTraversal pureTraversal;
    private Iterator<Traverser> currentIterator;

    public ServerResultStep(final Traversal.Admin<S,E> traversal, final Traversal overTheWireTraversal,
                            final ServerConnection serverConnection) {
        super(traversal);
        this.serverConnection = serverConnection;
        pureTraversal = new PureTraversal<>(overTheWireTraversal.asAdmin());
        this.integrateChild(pureTraversal.get());
    }

    @Override
    public List<Traversal.Admin<S, E>> getGlobalChildren() {
        return Collections.singletonList(pureTraversal.get());
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, pureTraversal.get());
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(pureTraversal.get());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Traverser processNextStart() throws NoSuchElementException {

        if (null == currentIterator) {
            try {
                final Traversal.Admin temp = pureTraversal.getPure();
                temp.setStrategies(this.getTraversal().getStrategies());
                currentIterator = serverConnection.submit(temp);
            } catch (ServerConnectionException sce) {
                throw new IllegalStateException(sce);
            }
        }

        return this.currentIterator.next();
    }
}
