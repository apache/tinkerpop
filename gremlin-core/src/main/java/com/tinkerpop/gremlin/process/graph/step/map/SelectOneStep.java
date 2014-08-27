package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.util.function.SBiFunction;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SelectOneStep<S, E> extends SelectStep {

    private final SBiFunction<Traverser<S>, Traversal.SideEffects, Map<String, E>> selectBiFunction;

    public SelectOneStep(final Traversal traversal, final String selectLabel, SFunction stepFunction) {
        super(traversal, Arrays.asList(selectLabel), stepFunction);
        this.selectBiFunction = this.biFunction;
        this.setBiFunction((traverser, sideEffects) -> this.selectBiFunction.apply((Traverser<S>)traverser, (Traversal.SideEffects)sideEffects).get(selectLabel));
    }
}
