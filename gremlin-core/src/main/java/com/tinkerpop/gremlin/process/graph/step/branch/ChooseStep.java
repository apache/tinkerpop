package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A step which offers a choice of two or more Traversals to take.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseStep<S, E, M> extends BranchStep<S, E, M> {

    public ChooseStep(final Traversal traversal, final Predicate<S> predicate, final Traversal<S, E> trueChoice, final Traversal<S, E> falseChoice) {
        this(traversal, (Function) s -> predicate.test((S) s));
        this.addFork((M) Boolean.TRUE, trueChoice);
        this.addFork((M) Boolean.FALSE, falseChoice);
    }

    public ChooseStep(final Traversal traversal, final Function<S, M> mapFunction) {
        super(traversal);
        this.setFunction(new Function<Traverser<S>, M>() {
            @Override
            public M apply(final Traverser<S> traverser) {
                return mapFunction.apply(traverser.get());
            }
        });
    }

    @Override
    public void addFork(final M pickToken, final Traversal<S, E> traversalFork) {
        if (Pick.any.equals(pickToken))
            throw new IllegalArgumentException("Choose step can not have an any-fork as only one fork per traverser is allowed");
        if (this.branches.containsKey(pickToken))
            throw new IllegalArgumentException("Choose step can only have one traversal per pick token: " + pickToken);
        super.addFork(pickToken, traversalFork);
    }

    /*@Override
    protected Iterator<Traverser<E>> standardAlgorithm() {
        while (true) {
            final Traverser<S> start = this.starts.next();
            final Traversal<S, E> choice = this.choices.get(this.mapFunction.apply(start.get()));
            if (null != choice) {
                choice.asAdmin().addStart(start);
                return choice.asAdmin().getEndStep();
            }
        }
    }

    @Override
    protected Iterator<Traverser<E>> computerAlgorithm() {
        while (true) {
            final Traverser<S> start = this.starts.next();
            final Traversal<S, E> choice = this.choices.get(this.mapFunction.apply(start.get()));
            if (null != choice) {
                start.asAdmin().setStepId(choice.asAdmin().getStartStep().getId());
                return IteratorUtils.of((Traverser) start);
            }
        }
    }*/

}
