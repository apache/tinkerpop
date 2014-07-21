package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.util.function.SFunction;
import com.tinkerpop.gremlin.util.function.SPredicate;

import java.util.Collections;
import java.util.Map;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ChooseStep<S, E, M> extends FlatMapStep<S, E> {

    public ChooseStep(final Traversal traversal, final SPredicate<Traverser<S>> ifPredicate, final Traversal<S, E> trueBranch, final Traversal<S, E> falseBranch) {
        super(traversal);
        this.setFunction(traverser -> {
            final Traversal<S, E> branch = ifPredicate.test(traverser) ? trueBranch : falseBranch;
            branch.addStarts(new SingleIterator<>(traverser));
            return branch;
        });
    }

    public ChooseStep(final Traversal traversal, final SFunction<Traverser<S>, M> mapFunction, final Map<M, Traversal<S, E>> branches) {
        super(traversal);
        this.setFunction(traverser -> {
            final Traversal<S, E> branch = branches.get(mapFunction.apply(traverser));
            if (null == branch) {
                return Collections.emptyIterator();
            } else {
                branch.addStarts(new SingleIterator<>(traverser));
                return branch;
            }
        });
    }
}
