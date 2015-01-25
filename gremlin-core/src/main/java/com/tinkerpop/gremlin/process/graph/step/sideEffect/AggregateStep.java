package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.AggregateMapReduce;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalLambda;
import com.tinkerpop.gremlin.process.util.TraversalObjectLambda;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AggregateStep<S> extends BarrierStep<S> implements SideEffectRegistrar, SideEffectCapable, Reversible, FunctionHolder<S, Object>, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT,
            TraverserRequirement.SIDE_EFFECTS
    ));

    private Function<S, Object> preAggregateFunction = s -> s;
    private String sideEffectKey;
    private boolean traversalFunction = false;

    public AggregateStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        AggregateStep.generateConsumer(this);
    }

    @Override
    public void registerSideEffects() {
        if (null == this.sideEffectKey) this.sideEffectKey = this.getId();
        this.getTraversal().asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, BulkSet::new);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> getMapReduce() {
        return new AggregateMapReduce(this);
    }

    @Override
    public void addFunction(final Function<S, Object> function) {
        this.preAggregateFunction = (this.traversalFunction = function instanceof TraversalObjectLambda) ?
                new TraversalLambda(((TraversalObjectLambda<S, Object>) function).getTraversal()) :
                function;
    }

    @Override
    public List<Function<S, Object>> getFunctions() {
        return Collections.singletonList(this.preAggregateFunction);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public AggregateStep<S> clone() throws CloneNotSupportedException {
        final AggregateStep<S> clone = (AggregateStep<S>) super.clone();
        clone.preAggregateFunction = CloneableLambda.cloneOrReturn(this.preAggregateFunction);
        AggregateStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S> void generateConsumer(final AggregateStep<S> aggregateStep) {
        aggregateStep.setConsumer(traverserSet ->
                traverserSet.forEach(traverser ->
                        TraversalHelper.addToCollection(
                                traverser.getSideEffects().get(aggregateStep.sideEffectKey),
                                aggregateStep.preAggregateFunction.apply(aggregateStep.traversalFunction ? (S) traverser : traverser.get()),
                                traverser.bulk())));
    }
}
