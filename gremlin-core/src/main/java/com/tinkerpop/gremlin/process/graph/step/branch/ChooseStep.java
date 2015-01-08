package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;
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
public final class ChooseStep<S, E, M> extends FlatMapStep<S, E> implements TraversalHolder<S, E> {

    private final Function<S, M> mapFunction;
    private Map<M, Traversal<S, E>> choices;

    public ChooseStep(final Traversal traversal, final Predicate<S> predicate, final Traversal<S, E> trueChoice, final Traversal<S, E> falseChoice) {
        this(traversal,
                (Function) s -> predicate.test((S) s),
                new HashMap() {{
                    put(Boolean.TRUE, trueChoice);
                    put(Boolean.FALSE, falseChoice);
                }});
    }

    public ChooseStep(final Traversal traversal, final Function<S, M> mapFunction, final Map<M, Traversal<S, E>> choices) {
        super(traversal);
        this.mapFunction = mapFunction;
        this.choices = choices;
        this.choices.values().forEach(choice -> choice.asAdmin().setStrategies(this.getTraversal().asAdmin().getStrategies()));
        ChooseStep.generateFunction(this);
    }

    public Function<S, M> getMapFunction() {
        return this.mapFunction;
    }

    public Map<M, Traversal<S, E>> getChoices() {
        return this.choices;
    }

    @Override
    public Collection<Traversal<S, E>> getTraversals() {
        return this.choices.values();
    }

    @Override
    public ChooseStep<S, E, M> clone() throws CloneNotSupportedException {
        final ChooseStep<S, E, M> clone = (ChooseStep<S, E, M>) super.clone();
        clone.choices = new HashMap<>();
        for (final Map.Entry<M, Traversal<S, E>> entry : this.choices.entrySet()) {
            clone.choices.put(entry.getKey(), entry.getValue());
        }
        ChooseStep.generateFunction(clone);
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.choices.toString());
    }

    ////////////////////////

    private static final <S, E, M> void generateFunction(final ChooseStep<S, E, M> chooseStep) {
        chooseStep.setFunction(traverser -> {
            final Traversal<S, E> branch = chooseStep.choices.get(chooseStep.mapFunction.apply(traverser.get()));
            if (null == branch) {
                return Collections.emptyIterator();
            } else {
                branch.asAdmin().addStart(traverser);
                return branch;
            }
        });
    }
}
