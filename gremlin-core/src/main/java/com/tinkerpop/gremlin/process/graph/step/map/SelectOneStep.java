package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectOneStep<S, E> extends SelectStep {

    private final Function<Traverser<S>, Map<String, E>> tempFunction;

    public SelectOneStep(final Traversal traversal, final String selectLabel, final Function stepFunction) {
        super(traversal, Arrays.asList(selectLabel), stepFunction);
        this.tempFunction = this.selectFunction;
        this.setFunction(traverser -> this.tempFunction.apply(((Traverser<S>) traverser)).get(selectLabel));
    }
}


