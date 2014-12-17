package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.ProfileMapReduce;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalMetricsUtil;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStep<S> extends SideEffectStep<S> implements Reversible, MapReducer<MapReduce.NullObject, TraversalMetricsUtil, MapReduce.NullObject, TraversalMetricsUtil, TraversalMetricsUtil> {

    private final String name;


    public ProfileStep(final Traversal traversal) {
        super(traversal);
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(TraversalMetrics.METRICS_KEY, this.traversal);
        this.name = null;
    }

    public ProfileStep(final Traversal<?, ?> traversal, final Step step) {
        super(traversal);
        // TODO: rjbriody - profile - is it ok to store the name here. Will it be set when using graph computer?
        this.name = step.toString();
    }

    @Override
    public void reset() {
        super.reset();
        this.getTraversal().sideEffects().remove(TraversalMetrics.METRICS_KEY);
    }


    @Override
    public MapReduce<MapReduce.NullObject, TraversalMetricsUtil, MapReduce.NullObject, TraversalMetricsUtil, TraversalMetricsUtil> getMapReduce() {
        return new ProfileMapReduce(this);
    }


    @Override
    public Traverser next() {
        // Wrap SideEffectStep's next() with timer.
        TraversalMetricsUtil traversalMetrics = this.getTraversal().sideEffects().getOrCreate(TraversalMetrics.METRICS_KEY, TraversalMetricsUtil::new);
        // TODO: rjbriod - profile - is it always true that hasNext() is called before next(). If so, remove this:
        traversalMetrics.initialize(this, this.getTraversal().asAdmin().getTraversalEngine().equals(TraversalEngine.STANDARD));

        Traverser<?> ret = null;
        traversalMetrics.start(this);
        try {
            ret = super.next();
            return ret;
        } finally {
            if (ret != null)
                traversalMetrics.finish(this, ret.asAdmin());
            else
                traversalMetrics.stop(this);
        }
    }

    @Override
    public boolean hasNext() {
        // Wrap SideEffectStep's hasNext() with timer.
        TraversalMetricsUtil traversalMetrics = this.getTraversal().sideEffects().getOrCreate(TraversalMetrics.METRICS_KEY, TraversalMetricsUtil::new);
        traversalMetrics.initialize(this, this.getTraversal().asAdmin().getTraversalEngine().equals(TraversalEngine.STANDARD));

        traversalMetrics.start(this);
        boolean ret = super.hasNext();
        traversalMetrics.stop(this);
        return ret;
    }

    public String getEventName() {
        return name;
    }
}
