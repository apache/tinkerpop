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
package com.tinkerpop.gremlin.process.graph.traversal.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraverserSet;

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CollectingBarrierStep<S> extends AbstractStep<S, S> {
    private TraverserSet<S> traverserSet = new TraverserSet<>();
    private Consumer<TraverserSet<S>> barrierConsumer;

    public CollectingBarrierStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setConsumer(final Consumer<TraverserSet<S>> barrierConsumer) {
        this.barrierConsumer = barrierConsumer;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    @Override
    public Traverser<S> processNextStart() {
        if (this.starts.hasNext()) {
            this.starts.forEachRemaining(this.traverserSet::add);
            if (null != this.barrierConsumer) this.barrierConsumer.accept(this.traverserSet);
        }
        return this.traverserSet.remove();
    }

    @Override
    public CollectingBarrierStep<S> clone() throws CloneNotSupportedException {
        final CollectingBarrierStep<S> clone = (CollectingBarrierStep<S>) super.clone();
        clone.traverserSet = new TraverserSet<>();
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.traverserSet.clear();
    }
}
