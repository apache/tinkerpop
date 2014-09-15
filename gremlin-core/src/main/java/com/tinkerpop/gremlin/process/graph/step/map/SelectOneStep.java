package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectOneStep<S, E> extends SelectStep {

    private final SFunction<Traverser<S>, Map<String, E>> selectFunction;

    public SelectOneStep(final Traversal traversal, final String selectLabel, final SFunction stepFunction) {
        super(traversal, Arrays.asList(selectLabel), stepFunction);
        this.selectFunction = this.function;
        this.setFunction(traverser -> this.selectFunction.apply(((Traverser<S>) traverser)).get(selectLabel));
    }
}


