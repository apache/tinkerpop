package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.FunctionHolder;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.TreeMapReduce;
import com.tinkerpop.gremlin.process.graph.util.Tree;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeStep<S> extends SideEffectStep<S> implements Reversible, SideEffectCapable, FunctionHolder<Object, Object>, MapReducer<Object, Tree, Object, Tree, Tree> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.PATH,
            TraverserRequirement.SIDE_EFFECTS
    ));

    private FunctionRing<Object, Object> functionRing;
    private final String sideEffectKey;

    public TreeStep(final Traversal traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = null == sideEffectKey ? this.getLabel().orElse(this.getId()) : sideEffectKey;
        this.functionRing = new FunctionRing<>();
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, Tree::new);
        TreeStep.generateConsumer(this);
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
        this.functionRing.reset();
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey);
    }

    @Override
    public TreeStep<S> clone() throws CloneNotSupportedException {
        final TreeStep<S> clone = (TreeStep<S>) super.clone();
        clone.functionRing = this.functionRing.clone();
        TreeStep.generateConsumer(clone);
        return clone;
    }

    @Override
    public void addFunction(final Function<Object, Object> function) {
        this.functionRing.addFunction(function);
    }

    @Override
    public List<Function<Object, Object>> getFunctions() {
        return this.functionRing.getFunctions();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    /////////////////////////

    private static final <S> void generateConsumer(final TreeStep<S> treeStep) {
        treeStep.setConsumer(traverser -> {
            Tree depth = traverser.sideEffects(treeStep.sideEffectKey);
            final Path path = traverser.path();
            for (int i = 0; i < path.size(); i++) {
                final Object object = treeStep.functionRing.next().apply(path.get(i));
                if (!depth.containsKey(object))
                    depth.put(object, new Tree<>());
                depth = (Tree) depth.get(object);
            }
            treeStep.functionRing.reset();
        });
    }
}
