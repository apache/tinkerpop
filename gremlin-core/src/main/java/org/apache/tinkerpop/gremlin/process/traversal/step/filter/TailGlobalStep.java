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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Set;

/**
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
public final class TailGlobalStep<S> extends AbstractStep<S, S> {

    private final long limit;
    private Deque<Traverser.Admin<S>> tail;

    public TailGlobalStep(final Traversal.Admin traversal, final long limit) {
        super(traversal);
        this.limit = limit;
        this.tail = new ArrayDeque<Traverser.Admin<S>>((int)limit);
    }

    @Override
    public Traverser<S> processNextStart() {
        if (this.starts.hasNext()) {
            this.starts.forEachRemaining(this::addTail);
        }
        return this.tail.pop();
    }

    @Override
    public void reset() {
        super.reset();
        this.tail.clear();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.limit);
    }

    @Override
    public TailGlobalStep<S> clone() {
        final TailGlobalStep<S> clone = (TailGlobalStep<S>) super.clone();
        clone.tail = new ArrayDeque<Traverser.Admin<S>>(this.tail);
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    private void addTail(Traverser.Admin<S> start) {
        if (this.tail.size() >= this.limit)
            this.tail.pop();
        this.tail.add(start);
    }
}
