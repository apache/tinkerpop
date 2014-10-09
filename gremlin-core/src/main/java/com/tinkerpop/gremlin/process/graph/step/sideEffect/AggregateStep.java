package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.AggregateMapReduce;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AggregateStep<S> extends BarrierStep<S> implements SideEffectCapable, Reversible, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> {

    private final Function<Traverser<S>, ?> preAggregateFunction;
    private final String sideEffectKey;

    public AggregateStep(final Traversal traversal, final String sideEffectKey, final Function<Traverser<S>, ?> preAggregateFunction) {
        super(traversal);
        this.preAggregateFunction = preAggregateFunction;
        this.sideEffectKey = null == sideEffectKey ? this.getLabel() : sideEffectKey;
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.setConsumer(traverserSet -> {
            final Collection<Object> aggregate = this.getTraversal().sideEffects().getOrCreate(this.sideEffectKey, BulkSet::new);
            traverserSet.forEach(traverser -> SideEffectStep.addToCollection(aggregate, null == this.preAggregateFunction ? traverser.get() : this.preAggregateFunction.apply(traverser), traverser.getBulk()));
        });
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return Graph.Key.isHidden(this.sideEffectKey) ? super.toString() : TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> getMapReduce() {
        return new AggregateMapReduce(this);
    }
}
