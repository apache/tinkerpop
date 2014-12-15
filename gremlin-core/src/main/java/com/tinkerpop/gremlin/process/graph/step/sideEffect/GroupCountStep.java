package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.FunctionRingAcceptor;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupCountMapReduce;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountStep<S> extends SideEffectStep<S> implements SideEffectCapable, Reversible, FunctionRingAcceptor<S, Object>, MapReducer<Object, Long, Object, Long, Map<Object, Long>> {

    private Function<S, ?> preGroupFunction = Function.identity();
    private final String sideEffectKey;

    public GroupCountStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = null == sideEffectKey ? this.getLabel() : sideEffectKey;
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.traversal.sideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMap<Object, Long>::new);
        this.setConsumer(traverser -> {
            final Map<Object, Long> groupCountMap = traverser.sideEffects().get(this.sideEffectKey);
            MapHelper.incr(groupCountMap, this.preGroupFunction.apply(traverser.get()), traverser.bulk());
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
        return Graph.System.isSystem(this.sideEffectKey) ? super.toString() : TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    @Override
    public void setFunctionRing(final FunctionRing<S, Object> functionRing) {
        FunctionRingAcceptor.singleFunctionSupported(functionRing, this);
        this.preGroupFunction = functionRing.next();
    }
}
