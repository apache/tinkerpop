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

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.VertexTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DependantMutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.StandardTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStep<S> extends AbstractStep<S, S> implements MapReducer<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> {

    // Stored in the Traversal sideEffects but kept here as a reference for convenience.
    private StandardTraversalMetrics traversalMetrics;

    public ProfileStep(final Traversal.Admin traversal) {
        super(traversal);
    }


    @Override
    public MapReduce<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> getMapReduce() {
        return ProfileMapReduce.instance();
    }

    @Override
    public Traverser<S> next() {
        Traverser<S> ret = null;
        initializeIfNeeded();
        traversalMetrics.start(this.getId());
        try {
            ret = super.next();
            return ret;
        } finally {
            if (ret != null) {
                traversalMetrics.finish(this.getId(), ret.asAdmin().bulk());
            } else {
                traversalMetrics.stop(this.getId());
            }
        }
    }

    @Override
    public boolean hasNext() {
        initializeIfNeeded();
        traversalMetrics.start(this.getId());
        boolean ret = super.hasNext();
        traversalMetrics.stop(this.getId());
        return ret;
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        return this.starts.next();
    }

    private void initializeIfNeeded() {
        if (traversalMetrics != null) {
            return;
        }

        createTraversalMetricsSideEffectIfNecessary();

        // How can traversalMetrics still be null? When running on computer it may need to be re-initialized from
        // sideEffects after serialization.
        if (traversalMetrics == null) {
            // look up the TraversalMetrics in the root traversal's sideEffects
            Traversal t = this.getTraversal();
            while (!(t.asAdmin().getParent() instanceof EmptyStep)) {
                t = t.asAdmin().getParent().asStep().getTraversal();
            }
            traversalMetrics = t.asAdmin().getSideEffects().<StandardTraversalMetrics>get(TraversalMetrics.METRICS_KEY).get();
        }
    }

    private void createTraversalMetricsSideEffectIfNecessary() {
        if (this.getTraversal().getSideEffects().get(TraversalMetrics.METRICS_KEY).isPresent()) {
            // Already initialized
            return;
        }

        if (!(this.getTraversal().getParent() instanceof EmptyStep)) {
            // Initialization is handled at the top-level of the traversal only.
            return;
        }

        // The following code is executed once per top-level (non-nested) Traversal for all Profile steps. (Technically,
        // once per thread if using Computer.)

        traversalMetrics = this.getTraversal().getSideEffects().getOrCreate(TraversalMetrics.METRICS_KEY, StandardTraversalMetrics::new);
        prepTraversalForProfiling(this.getTraversal().asAdmin(), null);
    }

    // Walk the traversal steps and initialize the Metrics timers.
    private void prepTraversalForProfiling(Traversal.Admin<?, ?> traversal, MutableMetrics parentMetrics) {

        DependantMutableMetrics prevMetrics = null;
        final List<Step> steps = traversal.getSteps();
        for (int ii = 0; ii + 1 < steps.size(); ii = ii + 2) {
            Step step = steps.get(ii);
            ProfileStep profileStep = (ProfileStep) steps.get(ii + 1);

            // Create metrics
            MutableMetrics metrics;

            // Computer metrics are "stand-alone" but Standard metrics handle double-counted upstream time.
            if (traversal.getEngine().isComputer()) {
                metrics = new MutableMetrics(step.getId(), step.toString());
            } else {
                metrics = new DependantMutableMetrics(step.getId(), step.toString(), prevMetrics);
                prevMetrics = (DependantMutableMetrics) metrics;
            }

            // Initialize counters (necessary because some steps might end up being 0)
            metrics.incrementCount(TraversalMetrics.ELEMENT_COUNT_ID, 0);
            metrics.incrementCount(TraversalMetrics.TRAVERSER_COUNT_ID, 0);

            // Add metrics to parent, if necessary
            if (parentMetrics != null) {
                parentMetrics.addNested(metrics);
            }

            // The TraversalMetrics sideEffect is shared across all the steps.
            profileStep.traversalMetrics = this.traversalMetrics;

            // Add root metrics to traversalMetrics
            this.traversalMetrics.addMetrics(metrics, step.getId(), ii / 2, parentMetrics == null, profileStep.getId());

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

    //////////////////

    public static final class ProfileMapReduce extends StaticMapReduce<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> {

        private static ProfileMapReduce INSTANCE = new ProfileMapReduce();

        private ProfileMapReduce() {

        }

        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public String getMemoryKey() {
            return TraversalMetrics.METRICS_KEY;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, StandardTraversalMetrics> emitter) {
            VertexTraversalSideEffects.of(vertex).<StandardTraversalMetrics>get(TraversalMetrics.METRICS_KEY).ifPresent(emitter::emit);
        }

        @Override
        public void combine(final NullObject key, final Iterator<StandardTraversalMetrics> values, final ReduceEmitter<NullObject, StandardTraversalMetrics> emitter) {
            reduce(key, values, emitter);
        }

        @Override
        public void reduce(final NullObject key, final Iterator<StandardTraversalMetrics> values, final ReduceEmitter<NullObject, StandardTraversalMetrics> emitter) {
            emitter.emit(StandardTraversalMetrics.merge(values));
        }

        @Override
        public StandardTraversalMetrics generateFinalResult(final Iterator<KeyValue<NullObject, StandardTraversalMetrics>> keyValues) {
            return keyValues.next().getValue();
        }

        public static ProfileMapReduce instance() {
            return INSTANCE;
        }
    }
}
