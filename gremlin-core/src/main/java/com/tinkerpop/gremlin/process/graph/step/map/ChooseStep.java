package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ChooseStep<S, E> extends FlatMapStep<S, E> {

    public final SFunction<Traverser<S>, Integer> chooseFunction;
    public final List<Traversal<S, E>> choices;

    public ChooseStep(final Traversal traversal, final SFunction<Traverser<S>, Integer> chooseFunction, final Traversal<S, E>... choices) {
        super(traversal);
        this.chooseFunction = chooseFunction;
        this.choices = Arrays.asList(choices);
        this.setFunction(traverser -> {
            final Integer choiceIndex = chooseFunction.apply(traverser);
            if (choiceIndex < 0 || choiceIndex >= this.choices.size())
                throw new IllegalStateException("The following choice index does not reference a traversal: " + choiceIndex);
            final Traversal<S, E> choice = this.choices.get(choiceIndex);
            choice.addStarts(new SingleIterator<>(traverser));
            return choice;
        });
    }
}
