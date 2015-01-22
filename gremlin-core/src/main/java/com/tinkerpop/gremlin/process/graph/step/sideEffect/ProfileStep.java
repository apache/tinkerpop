package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.ProfileMapReduce;
import com.tinkerpop.gremlin.process.util.StandardTraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStep<S> extends SideEffectStep<S> implements Reversible, MapReducer<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> {

    private final String name;

    public ProfileStep(final Traversal traversal) {
        super(traversal);
        this.name = null;
    }

    public ProfileStep(final Traversal<?, ?> traversal, final Step step) {
        super(traversal);
        this.name = step.toString();
    }

    @Override
    public MapReduce<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> getMapReduce() {
        return new ProfileMapReduce(this);
    }

    @Override
    public Traverser next() {
        // Wrap SideEffectStep's next() with timer.
        StandardTraversalMetrics traversalMetrics = getTraversalMetricsUtil();

        Traverser<?> ret = null;
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

    private StandardTraversalMetrics getTraversalMetricsUtil() {
        StandardTraversalMetrics traversalMetrics = this.getTraversal().asAdmin().getSideEffects().getOrCreate(TraversalMetrics.METRICS_KEY, StandardTraversalMetrics::new);
        final boolean isComputer = this.traversal.asAdmin().getTraversalEngine().get().equals(TraversalEngine.COMPUTER);
        traversalMetrics.initializeIfNecessary(this.getId(), this.traversal.asAdmin().getSteps().indexOf(this), name,  isComputer);
        return traversalMetrics;
    }
}
