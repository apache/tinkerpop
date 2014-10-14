package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A step which offers a choice of two or more Traversals to take
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseStep<S, E, M> extends FlatMapStep<S, E> {

    public ChooseStep(final Traversal traversal, final Predicate<Traverser<S>> ifPredicate, final Traversal<S, E> trueBranch, final Traversal<S, E> falseBranch) {
        super(traversal);
        this.setFunction(traverser -> {
            final Traversal<S, E> branch = ifPredicate.test(traverser) ? trueBranch : falseBranch;
            branch.addStart(traverser);
            return branch;
        });
    }

    public ChooseStep(final Traversal traversal, final Function<Traverser<S>, M> mapFunction, final Map<M, Traversal<S, E>> branches) {
        super(traversal);
        this.setFunction(traverser -> {
            final Traversal<S, E> branch = branches.get(mapFunction.apply(traverser));
            if (null == branch) {
                return Collections.emptyIterator();
            } else {
                branch.addStart(traverser);
                return branch;
            }
        });
    }
}
