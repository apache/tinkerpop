package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.TreeMapReduce;
import com.tinkerpop.gremlin.process.graph.util.Tree;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TreeStep<S> extends SideEffectStep<S> implements Reversible, PathConsumer, SideEffectCapable, VertexCentric, MapReducer<Object, Tree, Object, Tree, Tree> {

    public Tree tree;
    public FunctionRing functionRing;
    private final String memoryKey;
    private final String hiddenMemoryKey;

    public TreeStep(final Traversal traversal, final String memoryKey, final SFunction... branchFunctions) {
        super(traversal);
        this.memoryKey = null == memoryKey ? this.getAs() : memoryKey;
        this.hiddenMemoryKey = Graph.Key.hide(this.memoryKey);
        this.functionRing = new FunctionRing(branchFunctions);
        this.tree = traversal.sideEffects().getOrCreate(this.memoryKey, Tree::new);

        this.setConsumer(traverser -> {
            Tree depth = this.tree;
            final Path path = traverser.getPath();
            for (int i = 0; i < path.size(); i++) {
                final Object object = functionRing.next().apply(path.get(i));
                if (!depth.containsKey(object))
                    depth.put(object, new Tree<>());
                depth = (Tree) depth.get(object);
            }
            this.functionRing.reset();
        });
    }

    public String getMemoryKey() {
        return this.memoryKey;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.tree = vertex.<Tree>property(this.hiddenMemoryKey).orElse(new Tree());
        if (!vertex.property(this.hiddenMemoryKey).isPresent())
            vertex.property(this.hiddenMemoryKey, this.tree);
    }

    public MapReduce<Object, Tree, Object, Tree, Tree> getMapReduce() {
        return new TreeMapReduce(this);
    }
}
