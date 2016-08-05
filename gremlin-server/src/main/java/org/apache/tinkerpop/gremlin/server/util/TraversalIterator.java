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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalIterator implements Iterator<Object> {

    private final Iterator<Object> traversalIterator;
    private final HaltedTraverserStrategy haltedTraverserStrategy;

    public TraversalIterator(final Traversal.Admin traversal) {
        this.traversalIterator = traversal.getEndStep();
        this.haltedTraverserStrategy = traversal.getStrategies().getStrategy(HaltedTraverserStrategy.class).orElse(
                Boolean.valueOf(System.getProperty("is.testing", "false")) ?
                        HaltedTraverserStrategy.detached() :
                        HaltedTraverserStrategy.reference());
    }

    @Override
    public boolean hasNext() {
        return this.traversalIterator.hasNext();
    }

    @Override
    public Object next() {
        final Traverser.Admin t = this.haltedTraverserStrategy.halt((Traverser.Admin) traversalIterator.next());
        return new DefaultRemoteTraverser<>(t.get(), t.bulk());
    }
}