package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectOneStep<S, E> extends SelectStep {

    private final Function<Traverser<S>, Map<String, E>> selectFunction;

    public SelectOneStep(final Traversal traversal, final String selectLabel, final Function stepFunction) {
        super(traversal, Arrays.asList(selectLabel), stepFunction);
        this.selectFunction = this.function;
        this.setFunction(traverser -> this.selectFunction.apply(((Traverser<S>) traverser)).get(selectLabel));
    }
}


