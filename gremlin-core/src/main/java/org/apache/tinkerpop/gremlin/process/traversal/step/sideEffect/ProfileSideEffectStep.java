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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DependantMutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.util.function.StandardTraversalMetricsSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileSideEffectStep<S> extends SideEffectStep<S> implements SideEffectCapable<StandardTraversalMetrics, StandardTraversalMetrics>, GraphComputing {

    private String sideEffectKey;
    // Stored in the Traversal sideEffects but kept here as a reference for convenience.
    //private StandardTraversalMetrics traversalMetrics;
    private boolean onGraphComputer = false;

    public ProfileSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, (Supplier) StandardTraversalMetricsSupplier.instance(), Operator.assign);
    }

    @Override
    protected void sideEffect(Traverser.Admin<S> traverser) {

    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    private StandardTraversalMetrics getMetrics() {
        return this.getTraversal().getSideEffects().<StandardTraversalMetrics>get(this.sideEffectKey).get();
    }

    @Override
    public Traverser<S> next() {
        Traverser<S> ret = null;
        initializeIfNeeded();
        getMetrics().start(this.getId());
        try {
            ret = super.next();
            return ret;
        } finally {
            if (ret != null) {
                getMetrics().finish(this.getId(), ret.asAdmin().bulk());
            } else {
                getMetrics().stop(this.getId());
            }
        }
    }

    @Override
    public boolean hasNext() {
        initializeIfNeeded();
        this.getMetrics().start(this.getId());
        boolean ret = super.hasNext();
        this.getMetrics().stop(this.getId());
        return ret;
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        return this.starts.next();
    }

    private void initializeIfNeeded() {
        // Initialization is handled at the top-level of the traversal only.
        if (this.getMetrics().wasInitialized() ||
                !(this.getTraversal().getParent() instanceof TraversalVertexProgramStep) &&
                        !(this.getTraversal().getParent() instanceof EmptyStep)) {
            return;
        }
        // The following code is executed once per top-level (non-nested) Traversal for all Profile steps. (Technically,
        // once per thread if using Computer.)
        prepTraversalForProfiling(this.getTraversal().asAdmin(), null);
    }

    // Walk the traversal steps and initialize the Metrics timers.
    private void prepTraversalForProfiling(Traversal.Admin<?, ?> traversal, MutableMetrics parentMetrics) {

        DependantMutableMetrics prevMetrics = null;
        final List<Step> steps = traversal.getSteps();
        for (int ii = 0; ii + 1 < steps.size(); ii = ii + 2) {
            Step step = steps.get(ii);
            Step nextStep = steps.get(ii + 1);
            // Do not inject profiling after ProfileStep
            if (!(nextStep instanceof ProfileSideEffectStep)) {
                break;
            }
            ProfileSideEffectStep profileSideEffectStep = (ProfileSideEffectStep) nextStep;

            // Create metrics
            MutableMetrics metrics;

            // Computer metrics are "stand-alone" but Standard metrics handle double-counted upstream time.
            if (this.onGraphComputer) {
                metrics = new MutableMetrics(step.getId(), step.toString());
            } else {
                metrics = new DependantMutableMetrics(step.getId(), step.toString(), prevMetrics);
                prevMetrics = (DependantMutableMetrics) metrics;
            }

            if (step instanceof Profiling) {
                ((Profiling) step).setMetrics(metrics);
            }

            // Initialize counters (necessary because some steps might end up being 0)
            metrics.incrementCount(TraversalMetrics.ELEMENT_COUNT_ID, 0);
            metrics.incrementCount(TraversalMetrics.TRAVERSER_COUNT_ID, 0);

            // Add metrics to parent, if necessary
            if (parentMetrics != null) {
                parentMetrics.addNested(metrics);
            }

            // The TraversalMetrics sideEffect is shared across all the steps.
            //profileSideEffectStep.traversalMetrics = this.traversalMetrics;

            // Add root metrics to traversalMetrics
            this.getMetrics().addMetrics(metrics, step.getId(), ii / 2, parentMetrics == null, profileSideEffectStep.getId());

            // Handle nested traversal
            if (step instanceof TraversalParent) {
                for (Traversal.Admin<?, ?> t : ((TraversalParent) step).getLocalChildren()) {
                    prepTraversalForProfiling(t, metrics);
                }
                for (Traversal.Admin<?, ?> t : ((TraversalParent) step).getGlobalChildren()) {
                    prepTraversalForProfiling(t, metrics);
                }
            }
        }
    }

    ///////////////////////////

    /*public static final class ProfileBiOperator implements BinaryOperator<StandardTraversalMetrics>, Serializable {

        private static final ProfileBiOperator INSTANCE = new ProfileBiOperator();

        @Override
        public StandardTraversalMetrics apply(final StandardTraversalMetrics mutatingSeed, final StandardTraversalMetrics map) {
            return StandardTraversalMetrics.merge(IteratorUtils.of(mutatingSeed, map));
        }

        public static final ProfileBiOperator instance() {
            return INSTANCE;
        }
    }*/
}
