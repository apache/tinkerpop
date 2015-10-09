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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class CollectingBarrierStep<S> extends AbstractStep<S, S> implements Barrier {
    private TraverserSet<S> traverserSet = new TraverserSet<>();

    private int maxBarrierSize;

    public CollectingBarrierStep(final Traversal.Admin traversal) {
        this(traversal, Integer.MAX_VALUE);
    }

    public CollectingBarrierStep(final Traversal.Admin traversal, final int maxBarrierSize) {
        super(traversal);
        this.maxBarrierSize = maxBarrierSize;
    }

    public abstract void barrierConsumer(final TraverserSet<S> traverserSet);

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    @Override
    public void processAllStarts() {
        if (this.starts.hasNext()) {
            if (Integer.MAX_VALUE == this.maxBarrierSize) {
                this.starts.forEachRemaining(this.traverserSet::add);
                this.barrierConsumer(this.traverserSet);
            } else {
                while (this.starts.hasNext() && this.traverserSet.size() < this.maxBarrierSize) {
                    this.traverserSet.add(this.starts.next());
                }
                this.barrierConsumer(this.traverserSet);
            }
        }
    }

    @Override
    public Traverser<S> processNextStart() {
        if (!this.traverserSet.isEmpty()) {
            return this.traverserSet.remove();
        } else if (this.starts.hasNext()) {
            this.processAllStarts();
        }
        return this.traverserSet.remove();
    }

    @Override
    public CollectingBarrierStep<S> clone() {
        final CollectingBarrierStep<S> clone = (CollectingBarrierStep<S>) super.clone();
        clone.traverserSet = new TraverserSet<>();
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.maxBarrierSize == Integer.MAX_VALUE ? null : this.maxBarrierSize);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.maxBarrierSize;
    }

    @Override
    public void reset() {
        super.reset();
        this.traverserSet.clear();
    }
}
