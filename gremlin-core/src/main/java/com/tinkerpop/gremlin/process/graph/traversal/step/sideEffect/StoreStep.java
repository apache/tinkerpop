package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.StoreMapReduce;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traversal.util.TraversalUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StoreStep<S> extends SideEffectStep<S> implements SideEffectCapable, SideEffectRegistrar, Reversible, TraversalHolder, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

    private Traversal.Admin<S, Object> storeTraversal = new IdentityTraversal<>();
    private String sideEffectKey;

    public StoreStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        StoreStep.generateConsumer(this);
    }

    @Override
    public void registerSideEffects() {
        if (null == this.sideEffectKey) this.sideEffectKey = this.getId();
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, BulkSet::new);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.storeTraversal);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> getMapReduce() {
        return new StoreMapReduce(this);
    }

    @Override
    public List<Traversal<S, Object>> getLocalTraversals() {
        return Collections.singletonList(this.storeTraversal);
    }

    @Override
    public void addLocalTraversal(final Traversal.Admin<?, ?> storeTraversal) {
        this.storeTraversal = (Traversal.Admin<S, Object>) storeTraversal;
        this.executeTraversalOperations(this.storeTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = TraversalHolder.super.getRequirements();
        requirements.add(TraverserRequirement.SIDE_EFFECTS);
        requirements.add(TraverserRequirement.BULK);
        return requirements;
    }

    @Override
    public StoreStep<S> clone() throws CloneNotSupportedException {
        final StoreStep<S> clone = (StoreStep<S>) super.clone();
        clone.storeTraversal = this.storeTraversal.clone();
        clone.executeTraversalOperations(clone.storeTraversal, TYPICAL_LOCAL_OPERATIONS);
        StoreStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S> void generateConsumer(final StoreStep<S> storeStep) {
        storeStep.setConsumer(traverser -> TraversalHelper.addToCollection(
                traverser.sideEffects(storeStep.sideEffectKey),
                TraversalUtil.function(traverser.asAdmin(), storeStep.storeTraversal),
                traverser.bulk()));
    }
}
