package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.TreeStep;
import com.tinkerpop.gremlin.process.graph.util.Tree;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TreeMapReduce implements MapReduce<Object, Tree, Object, Tree, Tree> {

    public static final String TREE_STEP_MEMORY_KEY = "gremlin.treeStep.memoryKey";

    private String memoryKey;

    public TreeMapReduce() {

    }

    public TreeMapReduce(final TreeStep step) {
        this.memoryKey = step.getMemoryKey();
    }

    @Override
    public void storeState(final Configuration configuration) {
        configuration.setProperty(TREE_STEP_MEMORY_KEY, this.memoryKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.memoryKey = configuration.getString(TREE_STEP_MEMORY_KEY);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Object, Tree> emitter) {
        final Property<Tree> treeProperty = vertex.property(Graph.Key.hide(this.memoryKey));
        treeProperty.ifPresent(tree -> tree.splitParents().forEach(t -> emitter.emit(((Tree) t).keySet().iterator().next(), (Tree) t)));
    }

    @Override
    public Tree generateMemoryValue(final Iterator<Pair<Object, Tree>> keyValues) {
        final Tree result = new Tree();
        keyValues.forEachRemaining(pair -> result.addTree(pair.getValue1()));
        return result;
    }

    @Override
    public String getMemoryKey() {
        return this.memoryKey;
    }
}
