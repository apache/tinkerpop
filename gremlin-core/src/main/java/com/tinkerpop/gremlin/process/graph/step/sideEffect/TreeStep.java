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
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.util.function.SFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TreeStep<S> extends FilterStep<S> implements Reversible, PathConsumer, SideEffectCapable, Bulkable {

    public Tree tree;
    public FunctionRing functionRing;
    public String variable;

    public TreeStep(final Traversal traversal, final String variable, final SFunction... branchFunctions) {
        super(traversal);
        this.variable = variable;
        this.functionRing = new FunctionRing(branchFunctions);
        this.tree = traversal.memory().getOrCreate(variable, Tree::new);
        this.traversal.memory().set(this.variable, this.tree);

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

    public void setCurrentBulkCount(final long count) {
        // do nothing as repeated elements is not important for tree, only unique paths.
        // this is more of an optimization for not running the same path over and over again.
    }

    public String toString() {
        return this.variable.equals(SideEffectCapable.CAP_KEY) ?
                super.toString() :
                TraversalHelper.makeStepString(this, this.variable);
    }

    public String getVariable() {
        return this.variable;
    }
}
