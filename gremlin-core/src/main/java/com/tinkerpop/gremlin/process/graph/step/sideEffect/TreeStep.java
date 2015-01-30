package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectRegistrar;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.TreeMapReduce;
import com.tinkerpop.gremlin.process.graph.util.Tree;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.process.util.TraversalRing;
import com.tinkerpop.gremlin.process.util.TraversalUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeStep<S> extends SideEffectStep<S> implements SideEffectRegistrar, Reversible, SideEffectCapable, TraversalHolder, MapReducer<Object, Tree, Object, Tree, Tree> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.PATH,
            TraverserRequirement.SIDE_EFFECTS
    ));

    private TraversalRing<Object, Object> traversalRing;
    private String sideEffectKey;

    public TreeStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversalRing = new TraversalRing<>();
        TreeStep.generateConsumer(this);
    }

    @Override
    public void registerSideEffects() {
        if (null == this.sideEffectKey) this.sideEffectKey = this.getId();
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, Tree::new);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public MapReduce<Object, Tree, Object, Tree, Tree> getMapReduce() {
        return new TreeMapReduce(this);
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.traversalRing);
    }

    @Override
    public TreeStep<S> clone() throws CloneNotSupportedException {
        final TreeStep<S> clone = (TreeStep<S>) super.clone();
        clone.traversalRing = new TraversalRing<>();
        for (final Traversal.Admin<Object, Object> traversal : this.traversalRing.getTraversals()) {
            final Traversal.Admin<Object, Object> traversalClone = traversal.clone();
            clone.traversalRing.addTraversal(traversalClone);
            clone.executeTraversalOperations(traversalClone, TYPICAL_LOCAL_OPERATIONS);
        }
        TreeStep.generateConsumer(clone);
        return clone;
    }

    @Override
    public List<Traversal.Admin<Object, Object>> getLocalTraversals() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void addLocalTraversal(final Traversal.Admin<?, ?> treeTraversal) {
        this.traversalRing.addTraversal((Traversal.Admin<Object, Object>) treeTraversal);
        this.executeTraversalOperations(treeTraversal, TYPICAL_LOCAL_OPERATIONS);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;   // TODO: Bad
    }

    /////////////////////////

    private static final <S> void generateConsumer(final TreeStep<S> treeStep) {
        treeStep.setConsumer(traverser -> {
            Tree depth = traverser.sideEffects(treeStep.sideEffectKey);
            final Path path = traverser.path();
            for (int i = 0; i < path.size(); i++) {
                final Object object = TraversalUtil.function(path.<Object>get(i), treeStep.traversalRing.next());
                if (!depth.containsKey(object))
                    depth.put(object, new Tree<>());
                depth = (Tree) depth.get(object);
            }
            treeStep.traversalRing.reset();
        });
    }
}
