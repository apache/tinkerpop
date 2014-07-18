package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.util.function.SPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IfThenElseStep<S, E> extends FlatMapStep<S, E> {

    public final SPredicate<Traverser<S>> ifPredicate;
    public final Traversal<S, E> thenTraversal;
    public final Traversal<S, E> elseTraversal;

    public IfThenElseStep(final Traversal traversal, final SPredicate<Traverser<S>> ifPredicate, final Traversal<S, E> thenTraversal, final Traversal<S, E> elseTraversal) {
        super(traversal);
        this.ifPredicate = ifPredicate;
        this.thenTraversal = thenTraversal;
        this.elseTraversal = elseTraversal;
        this.setFunction(traverser -> {
            if (this.ifPredicate.test(traverser)) {
                this.thenTraversal.addStarts(new SingleIterator<>(traverser));
                return this.thenTraversal;
            } else {
                this.elseTraversal.addStarts(new SingleIterator<>(traverser));
                return this.elseTraversal;
            }
        });
    }
}
