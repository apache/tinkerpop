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

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.MemoryComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.function.BinaryOperator;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStep<S> extends AbstractStep<S, S> implements MemoryComputing<MutableMetrics> {  // pseudo GraphComputing but local traversals are "GraphComputing"
    private MutableMetrics metrics;
    private boolean onGraphComputer = false;

    public ProfileStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public MutableMetrics getMetrics() {
        return metrics;
    }

    @Override
    public Traverser.Admin<S> next() {
        Traverser.Admin<S> start = null;
        this.initializeIfNeeded();
        this.metrics.start();
        try {
            start = super.next();
            return start;
        } finally {
            if (start != null) {
                this.metrics.finish(start.bulk());
                if (this.onGraphComputer) {
                    this.getTraversal().getSideEffects().add(this.getId(), this.metrics);
                    this.metrics = null;
                }
            } else {
                this.metrics.stop();
                if (this.onGraphComputer) {
                    this.getTraversal().getSideEffects().add(this.getId(), this.metrics);
                    this.metrics = null;
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        initializeIfNeeded();
        this.metrics.start();
        boolean ret = super.hasNext();
        this.metrics.stop();
        return ret;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        return this.starts.next();
    }

    private void initializeIfNeeded() {
        if (null == this.metrics) {
            this.onGraphComputer = TraversalHelper.onGraphComputer(this.getTraversal());
            this.metrics = new MutableMetrics(this.getPreviousStep().getId(), this.getPreviousStep().toString());
            final Step<?, S> previousStep = this.getPreviousStep();

            // give metrics to the step being profiled so that it can add additional data to the metrics like
            // annotations
            if (previousStep instanceof Profiling)
                ((Profiling) previousStep).setMetrics(this.metrics);
        }
    }

    @Override
    public MemoryComputeKey<MutableMetrics> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), ProfileBiOperator.instance(), false, true);
    }

    @Override
    public ProfileStep<S> clone() {
        final ProfileStep<S> clone = (ProfileStep<S>) super.clone();
        clone.metrics = null;
        return clone;
    }

    /////

    public static class ProfileBiOperator implements BinaryOperator<MutableMetrics>, Serializable {

        private static final ProfileBiOperator INSTANCE = new ProfileBiOperator();

        @Override
        public MutableMetrics apply(final MutableMetrics metricsA, final MutableMetrics metricsB) {
            metricsA.aggregate(metricsB);
            return metricsA;
        }

        public static final ProfileBiOperator instance() {
            return INSTANCE;
        }
    }
}
