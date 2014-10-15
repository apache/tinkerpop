package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.ProfileMapReduce;
import com.tinkerpop.gremlin.process.util.GlobalMetrics;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

public final class ProfileStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible, MapReducer<MapReduce.NullObject, GlobalMetrics, MapReduce.NullObject, GlobalMetrics, GlobalMetrics> {

    public static final String METRICS_KEY = Graph.Key.hide("metrics");

    public ProfileStep(final Traversal traversal) {
        super(traversal);
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(METRICS_KEY, this.traversal);
    }

    @Override
    public String getSideEffectKey() {
        return METRICS_KEY;
    }

    @Override
    public MapReduce<MapReduce.NullObject, GlobalMetrics, MapReduce.NullObject, GlobalMetrics, GlobalMetrics> getMapReduce() {
        return new ProfileMapReduce();
    }
}