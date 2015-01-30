package com.tinkerpop.gremlin.process.graph.traversal.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.__;
import com.tinkerpop.gremlin.util.function.TraversableLambda;

import java.util.function.Function;

/**
 * A step which offers a choice of two or more Traversals to take.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseStep<S, E, M> extends BranchStep<S, E, M> {

    public ChooseStep(final Traversal traversal, final Traversal.Admin<S, M> choiceTraversal) {
        super(traversal);
        this.setBranchTraversal(choiceTraversal);
    }

    public ChooseStep(final Traversal traversal, final Traversal.Admin<S, ?> predicate, final Traversal.Admin<S, E> trueChoice, final Traversal.Admin<S, E> falseChoice) {
        this(traversal, __.<S>has(predicate).count().map(t -> t.get() == 0l ? (M) Boolean.FALSE : (M) Boolean.TRUE).asAdmin());
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

    private static class PredicateToFunction<S, E> implements Function<Traverser<S>, Boolean>, TraversableLambda, Cloneable {

        private Traversal.Admin<S, E> traversal;

        public PredicateToFunction(final Traversal<S, E> traversal) {
            this.traversal = traversal.asAdmin();
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
