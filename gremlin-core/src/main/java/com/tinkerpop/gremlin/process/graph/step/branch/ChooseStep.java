package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A step which offers a choice of two or more Traversals to take.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseStep<S, E, M> extends FlatMapStep<S, E> {

    private final Function<Traverser<S>, M> mapFunction;
    private final Map<M, Traversal<S, E>> choices;

    public ChooseStep(final Traversal traversal, final Predicate<Traverser<S>> predicate, final Traversal<S, E> trueChoice, final Traversal<S, E> falseChoice) {
        this(traversal,
                (Function) traverser -> predicate.test((Traverser) traverser),
                new HashMap() {{
                    put(Boolean.TRUE, trueChoice);
                    put(Boolean.FALSE, falseChoice);
                }});
    }

    public ChooseStep(final Traversal traversal, final Function<Traverser<S>, M> mapFunction, final Map<M, Traversal<S, E>> choices) {
        super(traversal);
        this.mapFunction = mapFunction;
        this.choices = choices;
        this.setFunction(traverser -> {
            final Traversal<S, E> branch = this.choices.get(this.mapFunction.apply(traverser));
            if (null == branch) {
                return Collections.emptyIterator();
            } else {
                branch.addStart(traverser);
                return branch;
            }
        });
    }

    public Function<Traverser<S>, M> getMapFunction() {
        return this.mapFunction;
    }

    public Map<M, Traversal<S, E>> getChoices() {
        return this.choices;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.choices.toString());
    }

}
