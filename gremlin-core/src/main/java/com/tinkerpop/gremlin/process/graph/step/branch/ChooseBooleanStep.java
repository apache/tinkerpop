package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseBooleanStep<S, E> extends FlatMapStep<S, E> {

    private final Predicate<Traverser<S>> choosePredicate;
    private final Traversal<S, E> trueChoice;
    private final Traversal<S, E> falseChoice;

    public ChooseBooleanStep(final Traversal traversal, final Predicate<Traverser<S>> choosePredicate, final Traversal<S, E> trueChoice, final Traversal<S, E> falseChoice) {
        super(traversal);
        this.choosePredicate = choosePredicate;
        this.trueChoice = trueChoice;
        this.falseChoice = falseChoice;
        this.setFunction(traverser -> {
            final Traversal<S, E> branch = this.choosePredicate.test(traverser) ? this.trueChoice : this.falseChoice;
            branch.addStart(traverser);
            return branch;
        });
    }

    public Predicate<Traverser<S>> getChoosePredicate() {
        return this.choosePredicate;
    }

    public Traversal<S, E> getTrueChoice() {
        return this.trueChoice;
    }

    public Traversal<S, E> getFalseChoice() {
        return this.falseChoice;
    }
}