package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.GroupCountMapReduce;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.traversal.IdentityTraversal;
import com.tinkerpop.gremlin.process.util.MapHelper;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountStep<S> extends SideEffectStep<S> implements SideEffectRegistrar, SideEffectCapable, Reversible, TraversalHolder, MapReducer<Object, Long, Object, Long, Map<Object, Long>> {

    private Traversal.Admin<S, Object> groupTraversal = new IdentityTraversal<>();
    private String sideEffectKey;
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
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.groupTraversal);
    }

    @Override
    public void addLocalTraversal(final Traversal.Admin<?, ?> traversal) {
        this.groupTraversal = (Traversal.Admin<S, Object>) traversal;
        this.executeTraversalOperations(this.groupTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public List<Traversal<S, Object>> getLocalTraversals() {
        return Collections.singletonList(this.groupTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        final Set<TraverserRequirement> requirements = TraversalHolder.super.getRequirements();
        requirements.add(TraverserRequirement.BULK);
        requirements.add(TraverserRequirement.SIDE_EFFECTS);
        return requirements;
    }

    @Override
    public GroupCountStep<S> clone() throws CloneNotSupportedException {
        final GroupCountStep<S> clone = (GroupCountStep<S>) super.clone();
        clone.groupTraversal = this.groupTraversal.clone().asAdmin();
        clone.executeTraversalOperations(clone.groupTraversal, TYPICAL_LOCAL_OPERATIONS);
        GroupCountStep.generateConsumer(clone);
        return clone;
    }

    /////////////////////////

    private static final <S> void generateConsumer(final GroupCountStep<S> groupCountStep) {
        groupCountStep.setConsumer(traverser -> {
            final Map<Object, Long> groupCountMap = traverser.sideEffects(groupCountStep.sideEffectKey);
            MapHelper.incr(groupCountMap, TraversalUtil.function(traverser.asAdmin(), groupCountStep.groupTraversal), traverser.bulk());
        });
    }
}
