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
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraverserIterator implements Iterator<Object> {

    private final Traversal.Admin traversal;
    private final HaltedTraverserStrategy haltedTraverserStrategy;
    private final TraverserSet bulker = new TraverserSet();
    private final int barrierSize;

    public TraverserIterator(final Traversal.Admin traversal) {
        this.traversal = traversal;
        this.barrierSize = traversal.getTraverserRequirements().contains(TraverserRequirement.ONE_BULK) ? 1 : 1000;
        this.haltedTraverserStrategy = traversal.getStrategies().getStrategy(HaltedTraverserStrategy.class).orElse(
                Boolean.valueOf(System.getProperty("is.testing", "false")) ?
                        HaltedTraverserStrategy.detached() :
                        HaltedTraverserStrategy.reference());
    }

    public Traversal.Admin getTraversal() {
        return this.traversal;
    }

    @Override
    public boolean hasNext() {
        if (this.bulker.isEmpty())
            this.fillBulker();
        return !this.bulker.isEmpty();
    }

    @Override
    public Object next() {
        if (this.bulker.isEmpty())
            this.fillBulker();
        final Traverser.Admin t = this.haltedTraverserStrategy.halt(this.bulker.remove());
        return new DefaultRemoteTraverser<>(t.get(), t.bulk());
    }

    private final void fillBulker() {
        while (this.traversal.hasNext() && this.bulker.size() < this.barrierSize) {
            this.bulker.add(this.traversal.nextTraverser());
        }
    }
}