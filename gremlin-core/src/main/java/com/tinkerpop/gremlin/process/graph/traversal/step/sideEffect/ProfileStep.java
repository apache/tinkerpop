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
package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.traversal.step.MapReducer;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.ProfileMapReduce;
import com.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import com.tinkerpop.gremlin.process.util.metric.StandardTraversalMetrics;
import com.tinkerpop.gremlin.process.util.metric.TraversalMetrics;

import java.util.NoSuchElementException;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStep<S> extends AbstractStep<S, S> implements Reversible, MapReducer<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> {

    private final String name;

    public ProfileStep(final Traversal.Admin traversal) {
        super(traversal);
        this.name = null;
    }

    public ProfileStep(final Traversal.Admin traversal, final Step step) {
        super(traversal);
        this.name = step.toString();
    }

    @Override
    public MapReduce<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> getMapReduce() {
        return new ProfileMapReduce();
    }

    @Override
    public Traverser<S> next() {
        // Wrap SideEffectStep's next() with timer.
        StandardTraversalMetrics traversalMetrics = getTraversalMetricsUtil();

        Traverser<S> ret = null;
        traversalMetrics.start(this.getId());
        try {
            ret = super.next();
            return ret;
        } finally {
            if (ret != null)
                traversalMetrics.finish(this.getId(), ret.asAdmin().bulk());
            else
                traversalMetrics.stop(this.getId());
        }
    }

    @Override
    public boolean hasNext() {
        // Wrap SideEffectStep's hasNext() with timer.
        StandardTraversalMetrics traversalMetrics = getTraversalMetricsUtil();
        traversalMetrics.start(this.getId());
        boolean ret = super.hasNext();
        traversalMetrics.stop(this.getId());
        return ret;
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        return this.starts.next();
    }

    private StandardTraversalMetrics getTraversalMetricsUtil() {
        StandardTraversalMetrics traversalMetrics = this.getTraversal().asAdmin().getSideEffects().getOrCreate(TraversalMetrics.METRICS_KEY, StandardTraversalMetrics::new);
        final boolean isComputer = this.traversal.asAdmin().getEngine().get().equals(TraversalEngine.COMPUTER);
        traversalMetrics.initializeIfNecessary(this.getId(), this.traversal.asAdmin().getSteps().indexOf(this), name, isComputer);
        return traversalMetrics;
    }
}
