package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeStep;
import com.tinkerpop.gremlin.process.graph.util.Tree;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TreeMapReduce implements MapReduce<Object, Tree, Object, Tree, Tree> {

    public static final String TREE_STEP_SIDE_EFFECT_KEY = "gremlin.treeStep.sideEffectKey";

    private String sideEffectKey;
    private Traversal traversal;

    private TreeMapReduce() {

    }

    public TreeMapReduce(final TreeStep step) {
        this.sideEffectKey = step.getSideEffectKey();
        this.traversal = step.getTraversal();
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
        configuration.setProperty(TREE_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(TREE_STEP_SIDE_EFFECT_KEY);
        this.traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Object, Tree> emitter) {
        this.traversal.asAdmin().getSideEffects().setLocalVertex(vertex);
        this.traversal.asAdmin().getSideEffects().<Tree<?>>ifPresent(this.sideEffectKey, tree -> tree.splitParents().forEach(branches -> emitter.emit(branches.keySet().iterator().next(), branches)));
    }

    @Override
    public Tree generateFinalResult(final Iterator<KeyValue<Object, Tree>> keyValues) {
        final Tree result = new Tree();
        keyValues.forEachRemaining(keyValue -> result.addTree(keyValue.getValue()));
        return result;
    }

    @Override
    public String getMemoryKey() {
        return this.sideEffectKey;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + this.sideEffectKey).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.sideEffectKey);
    }

    @Override
    public TreeMapReduce clone() throws CloneNotSupportedException {
        final TreeMapReduce clone = (TreeMapReduce) super.clone();
        clone.traversal = this.traversal.clone();
        return clone;
    }
}
