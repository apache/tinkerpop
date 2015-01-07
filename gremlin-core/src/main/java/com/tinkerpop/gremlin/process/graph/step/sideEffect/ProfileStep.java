package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.ProfileMapReduce;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetricsUtil;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible, MapReducer<MapReduce.NullObject, TraversalMetricsUtil, MapReduce.NullObject, TraversalMetricsUtil, TraversalMetricsUtil> {

    private final String name;

    public ProfileStep(final Traversal traversal) {
        super(traversal);
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(TraversalMetrics.METRICS_KEY, this.traversal);
        this.name = null;
    }

    public ProfileStep(final Traversal<?, ?> traversal, final Step step) {
        super(traversal);
        this.name = step.toString();
    }

    @Override
    public void reset() {
        super.reset();
        this.getTraversal().asAdmin().getSideEffects().remove(TraversalMetrics.METRICS_KEY);
    }


    @Override
    public MapReduce<MapReduce.NullObject, TraversalMetricsUtil, MapReduce.NullObject, TraversalMetricsUtil, TraversalMetricsUtil> getMapReduce() {
        return new ProfileMapReduce(this);
    }


    @Override
    public Traverser next() {
        // Wrap SideEffectStep's next() with timer.
        TraversalMetricsUtil traversalMetrics = getTraversalMetricsUtil();

        Traverser<?> ret = null;
        traversalMetrics.start(this.getLabel());
        try {
            ret = super.next();
            return ret;
        } finally {
            if (ret != null)
                traversalMetrics.finish(this.getLabel(), ret.asAdmin().bulk());
            else
                traversalMetrics.stop(this.getLabel());
        }
    }

    @Override
    public boolean hasNext() {
        // Wrap SideEffectStep's hasNext() with timer.
        TraversalMetricsUtil traversalMetrics = getTraversalMetricsUtil();

        traversalMetrics.start(this.getLabel());
        boolean ret = super.hasNext();
        traversalMetrics.stop(this.getLabel());
        return ret;
    }

    private TraversalMetricsUtil getTraversalMetricsUtil() {
        TraversalMetricsUtil traversalMetrics = this.getTraversal().asAdmin().getSideEffects().getOrCreate(TraversalMetrics.METRICS_KEY, TraversalMetricsUtil::new);
        traversalMetrics.initializeIfNecessary(this.getLabel(), this.traversal.asAdmin().getSteps().indexOf(this), name);
        return traversalMetrics;
    }

    public String getEventName() {
        return name;
    }

    @Override
    public String getSideEffectKey(){
        return TraversalMetrics.METRICS_KEY;
    }
}
