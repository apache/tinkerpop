package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.ProfileMapReduce;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStep<S> extends SideEffectStep<S> implements Reversible, MapReducer<MapReduce.NullObject, TraversalMetrics, MapReduce.NullObject, TraversalMetrics, TraversalMetrics> {

    public static final String METRICS_KEY = Graph.System.system("metrics");
    public final String name;

    public ProfileStep(final Traversal traversal) {
        super(traversal);
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(METRICS_KEY, this.traversal);
        this.name = null;
    }

    public ProfileStep(final Traversal<?, ?> traversal, final Step step) {
        super(traversal);
        // TODO: rjbriody - profile is it wrong to store name here like this?
        this.name = step.toString();
    }

    @Override
    public MapReduce<MapReduce.NullObject, TraversalMetrics, MapReduce.NullObject, TraversalMetrics, TraversalMetrics> getMapReduce() {
        return new ProfileMapReduce(this);
    }


    @Override
    public void reset() {
        super.reset();
        this.getTraversal().sideEffects().remove(METRICS_KEY);
    }


    @Override
    public Traverser next() {
        // Wrap SideEffectStep's next() with timer.
        TraversalMetrics traversalMetrics = this.getTraversal().sideEffects().getOrCreate(METRICS_KEY, TraversalMetrics::new);
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
        TraversalMetrics traversalMetrics = this.getTraversal().sideEffects().getOrCreate(METRICS_KEY, TraversalMetrics::new);
        traversalMetrics.start(this);
        boolean ret = super.hasNext();
        traversalMetrics.stop(this);
        return ret;
    }

    public String getEventName() {
        return name;
    }
}
