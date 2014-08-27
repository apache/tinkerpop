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
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TreeStep<S> extends SideEffectStep<S> implements Reversible, PathConsumer, SideEffectCapable, VertexCentric, MapReducer<Object, Tree, Object, Tree, Tree> {

    public Tree tree;
    public FunctionRing functionRing;
    private final String sideEffectKey;
    private final String hiddenSideEffectKey;

    public TreeStep(final Traversal traversal, final String sideEffectKey, final SFunction... branchFunctions) {
        super(traversal);
        this.sideEffectKey = null == sideEffectKey ? this.getLabel() : sideEffectKey;
        this.hiddenSideEffectKey = Graph.Key.hide(this.sideEffectKey);
        this.functionRing = new FunctionRing(branchFunctions);
        TraversalHelper.verifySideEffectKeyIsNotAStepLabel(this.sideEffectKey, this.traversal);
        this.tree = traversal.sideEffects().getOrCreate(this.sideEffectKey, Tree::new);

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

    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    public void setCurrentVertex(final Vertex vertex) {
        this.tree = vertex.<Tree>property(this.hiddenSideEffectKey).orElse(new Tree());
        if (!vertex.property(this.hiddenSideEffectKey).isPresent())
            vertex.property(this.hiddenSideEffectKey, this.tree);
    }

    public MapReduce<Object, Tree, Object, Tree, Tree> getMapReduce() {
        return new TreeMapReduce(this);
    }

    @Override
    public void reset() {
        super.reset();
        this.functionRing.reset();
    }
}
