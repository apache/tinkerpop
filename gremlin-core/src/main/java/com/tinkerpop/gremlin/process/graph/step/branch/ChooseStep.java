package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.TraversalLambda;
import com.tinkerpop.gremlin.process.util.TraversalObjectLambda;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A step which offers a choice of two or more Traversals to take.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseStep<S, E, M> extends BranchStep<S, E, M> {

    public ChooseStep(final Traversal traversal, final Function<S, M> choiceFunction) {
        super(traversal);
        this.setFunction(choiceFunction instanceof TraversalObjectLambda ?
                new TraversalLambda<>(((TraversalObjectLambda<S, M>) choiceFunction).getTraversal()) :
                new Function<Traverser<S>, M>() {
                    @Override
                    public M apply(final Traverser<S> traverser) {
                        return choiceFunction.apply(traverser.get());
                    }
                });
    }

    public ChooseStep(final Traversal traversal, final Predicate<S> predicate, final Traversal<S, E> trueChoice, final Traversal<S, E> falseChoice) {
        this(traversal, (Function) s -> predicate.test((S) s));
        this.addOption((M) Boolean.TRUE, trueChoice);
        this.addOption((M) Boolean.FALSE, falseChoice);
    }

    @Override
    public void addOption(final M pickToken, final Traversal<S, E> traversalOption) {
        if (Pick.any.equals(pickToken))
            throw new IllegalArgumentException("Choose step can not have an any-option as only one option per traverser is allowed");
        if (this.traversalOptions.containsKey(pickToken))
            throw new IllegalArgumentException("Choose step can only have one traversal per pick token: " + pickToken);
        super.addOption(pickToken, traversalOption);
    }
}
