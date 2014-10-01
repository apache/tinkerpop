package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupCountMapReduce;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GroupCountStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible, MapReducer<Object, Long, Object, Long, Map<Object, Long>> {

    public Function<Traverser<S>, ?> preGroupFunction;
    private final String sideEffectKey;

    public GroupCountStep(final Traversal traversal, final String sideEffectKey, final Function<Traverser<S>, ?> preGroupFunction) {
        super(traversal);
        this.preGroupFunction = preGroupFunction;
        this.sideEffectKey = null == sideEffectKey ? this.getLabel() : sideEffectKey;
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.getTraversal().sideEffects().getOrCreate(this.sideEffectKey, HashMap::new); // TODO: for standard
        this.setConsumer(traverser -> {
            final Map<Object, Long> groupCountMap = traverser.getSideEffects().getOrCreate(this.sideEffectKey, HashMap::new);
            MapHelper.incr(groupCountMap,
                    null == this.preGroupFunction ? traverser.get() : this.preGroupFunction.apply(traverser),
                    ((Traverser.System) traverser).getBulk());
        });
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public MapReduce<Object, Long, Object, Long, Map<Object, Long>> getMapReduce() {
        return new GroupCountMapReduce(this);
    }

    @Override
    public String toString() {
        return Graph.Key.isHidden(this.sideEffectKey) ? super.toString() : TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

}
