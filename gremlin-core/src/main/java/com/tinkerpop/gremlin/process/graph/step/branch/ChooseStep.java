package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.TraversalLambda;
import com.tinkerpop.gremlin.process.util.TraversalObjectLambda;
import com.tinkerpop.gremlin.util.function.TraversableLambda;

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
        if (choiceFunction instanceof TraversalObjectLambda) {
            this.setFunction(new TraversalLambda<>(((TraversalObjectLambda<S, M>) choiceFunction).getTraversal()));
        } else if (choiceFunction instanceof PredicateToFunction) {
            this.setFunction((PredicateToFunction) choiceFunction);
        } else {
            this.setFunction(new Function<Traverser<S>, M>() {
                @Override
                public M apply(final Traverser<S> traverser) {
                    return choiceFunction.apply(traverser.get());
                }
            });
        }
    }

    public ChooseStep(final Traversal traversal, final Predicate<S> predicate, final Traversal<S, E> trueChoice, final Traversal<S, E> falseChoice) {
        this(traversal, (Function) s -> predicate.test((S) s));
        this.addOption((M) Boolean.TRUE, trueChoice);
        this.addOption((M) Boolean.FALSE, falseChoice);
    }

    public ChooseStep(final Traversal traversal, final Traversal<S, ?> predicate, final Traversal<S, E> trueChoice, final Traversal<S, E> falseChoice) {
        this(traversal, (Function<S, M>) new PredicateToFunction<>(predicate));
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

    ////

    private static class PredicateToFunction<S, E> implements Function<Traverser<S>, Boolean>, TraversableLambda<S, E>, Cloneable {

        private Traversal.Admin<S, E> traversal;

        public PredicateToFunction(final Traversal<S, E> traversal) {
            this.traversal = traversal.asAdmin();
        }

        @Override
        public PredicateToFunction<S, E> cloneLambda() throws CloneNotSupportedException {
            return this.clone();
        }

        @Override
        public Boolean apply(final Traverser<S> traverser) {
            final Traverser.Admin<S> split = traverser.asAdmin().split();
            split.setSideEffects(this.traversal.getSideEffects());
            this.traversal.reset();
            this.traversal.addStart(split);
            return this.traversal.hasNext();
        }

        @Override
        public Traversal<S, E> getTraversal() {
            return this.traversal;
        }

        @Override
        public void reset() {
            this.traversal.reset();
        }

        @Override
        public PredicateToFunction<S, E> clone() throws CloneNotSupportedException {
            final PredicateToFunction<S, E> clone = (PredicateToFunction<S, E>) super.clone();
            clone.traversal = this.traversal.clone().asAdmin();
            return clone;
        }

        @Override
        public String toString() {
            return this.traversal.toString();
        }
    }
}
