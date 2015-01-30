package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.traversal.TraversalParent;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.AggregateMapReduce;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.CollectingBarrierStep;
import com.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.BulkSet;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AggregateStep<S> extends CollectingBarrierStep<S> implements SideEffectRegistrar, SideEffectCapable, Reversible, TraversalParent, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

    private Traversal.Admin<S, Object> aggregateTraversal = new IdentityTraversal<>();
    private String sideEffectKey;

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
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.aggregateTraversal);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> getMapReduce() {
        return new AggregateMapReduce(this);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> traversal) {
        this.aggregateTraversal = this.integrateChild(traversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public List<Traversal.Admin<S, Object>> getLocalChildren() {
        return Collections.singletonList(this.aggregateTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public AggregateStep<S> clone() throws CloneNotSupportedException {
        final AggregateStep<S> clone = (AggregateStep<S>) super.clone();
        clone.aggregateTraversal = this.integrateChild(this.aggregateTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        AggregateStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S> void generateConsumer(final AggregateStep<S> aggregateStep) {
        aggregateStep.setConsumer(traverserSet ->
                traverserSet.forEach(traverser ->
                        TraversalHelper.addToCollection(
                                traverser.getSideEffects().get(aggregateStep.sideEffectKey),
                                TraversalUtil.apply(traverser, aggregateStep.aggregateTraversal),
                                traverser.bulk())));
    }
}
