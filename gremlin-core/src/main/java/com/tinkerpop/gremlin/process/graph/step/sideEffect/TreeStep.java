package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.filter.FilterStep;
import com.tinkerpop.gremlin.process.graph.step.util.Tree;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TreeStep<S> extends FilterStep<S> implements Reversible, PathConsumer, SideEffectCapable, Bulkable {

    public Tree tree;
    public FunctionRing functionRing;

    public TreeStep(final Traversal traversal, final SFunction... branchFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing(branchFunctions);
        this.tree = traversal.memory().getOrCreate(this.getAs(), Tree::new);

        this.setPredicate(traverser -> {
            Tree depth = this.tree;
            final Path path = traverser.getPath();
            for (int i = 0; i < path.size(); i++) {
                final Object object = functionRing.next().apply(path.get(i));
                if (!depth.containsKey(object))
                    depth.put(object, new Tree<>());
                depth = (Tree) depth.get(object);
            }
            return true;
        });
    }

    @Override
    public void setAs(final String as) {
        this.traversal.memory().move(this.getAs(), as, Tree::new);
        super.setAs(as);
    }

    public void setCurrentBulkCount(final long count) {
        // do nothing as repeated elements is not important for tree, only unique paths.
        // this is more of an optimization for not running the same path over and over again.
    }
}
