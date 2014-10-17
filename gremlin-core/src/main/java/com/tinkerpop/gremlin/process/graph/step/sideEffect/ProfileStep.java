package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.ProfileMapReduce;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Graph;

/**
 * @author Bob Briody (http://bobbriody.com)
 */
public final class ProfileStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible, MapReducer<MapReduce.NullObject, TraversalMetrics, MapReduce.NullObject, TraversalMetrics, TraversalMetrics> {

    public static final String METRICS_KEY = Graph.System.system("metrics");

    public ProfileStep(final Traversal traversal) {
        super(traversal);
        if (!PROFILING_ENABLED) {
            throw new IllegalStateException("The profile()-step can only be used when profiling is enabled via " +
                    "'gremlin.sh -p' or directly via -Dtinkerpop.profiling=true");
        }
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(METRICS_KEY, this.traversal);
    }

    @Override
    public String getSideEffectKey() {
        return METRICS_KEY;
    }

    @Override
    public MapReduce<MapReduce.NullObject, TraversalMetrics, MapReduce.NullObject, TraversalMetrics, TraversalMetrics> getMapReduce() {
        return new ProfileMapReduce();
    }
}