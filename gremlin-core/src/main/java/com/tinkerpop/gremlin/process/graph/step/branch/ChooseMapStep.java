package com.tinkerpop.gremlin.process.graph.step.branch;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * A step which offers a choice of two or more Traversals to take
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChooseMapStep<S, E, M> extends FlatMapStep<S, E> {

    private final Function<Traverser<S>, M> mapFunction;
    private final Map<M, Traversal<S, E>> choices;


    public ChooseMapStep(final Traversal traversal, final Function<Traverser<S>, M> mapFunction, final Map<M, Traversal<S, E>> choices) {
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
