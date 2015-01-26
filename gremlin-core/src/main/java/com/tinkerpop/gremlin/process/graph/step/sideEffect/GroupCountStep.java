package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupCountMapReduce;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalObjectLambda;
import com.tinkerpop.gremlin.util.function.CloneableLambda;
import com.tinkerpop.gremlin.util.function.ResettableLambda;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountStep<S> extends SideEffectStep<S> implements SideEffectRegistrar, SideEffectCapable, Reversible, FunctionHolder<S, Object>, MapReducer<Object, Long, Object, Long, Map<Object, Long>> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT,
            TraverserRequirement.SIDE_EFFECTS
    ));

    private Function<S, Object> preGroupFunction = s -> s;
    private String sideEffectKey;
    private boolean traversalFunction = false;
    // TODO: onFirst like subgraph so we don't keep getting the map

    public GroupCountStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        GroupCountStep.generateConsumer(this);
    }

    @Override
    public void registerSideEffects() {
        if (this.sideEffectKey == null) this.sideEffectKey = this.getId();
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMap<Object, Long>::new);
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
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    @Override
    public void addFunction(final Function<S, Object> function) {
        this.preGroupFunction = (this.traversalFunction = function instanceof TraversalObjectLambda) ?
                ((TraversalObjectLambda) function).asTraversalLambda() :
                function;
    }

    @Override
    public List<Function<S, Object>> getFunctions() {
        return Collections.singletonList(this.preGroupFunction);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public GroupCountStep<S> clone() throws CloneNotSupportedException {
        final GroupCountStep<S> clone = (GroupCountStep<S>) super.clone();
        clone.preGroupFunction = CloneableLambda.cloneOrReturn(this.preGroupFunction);
        GroupCountStep.generateConsumer(clone);
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        ResettableLambda.resetOrReturn(this.preGroupFunction);
    }

    /////////////////////////

    private static final <S> void generateConsumer(final GroupCountStep<S> groupCountStep) {
        groupCountStep.setConsumer(traverser -> {
            final Map<Object, Long> groupCountMap = traverser.sideEffects(groupCountStep.sideEffectKey);
            MapHelper.incr(groupCountMap, groupCountStep.preGroupFunction.apply(groupCountStep.traversalFunction ?
                    (S) traverser :
                    traverser.get()), traverser.bulk());
        });
    }
}
