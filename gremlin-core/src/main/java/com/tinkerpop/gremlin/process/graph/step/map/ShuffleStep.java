package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;

import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ShuffleStep<S> extends BarrierStep<S> {

    public ShuffleStep(final Traversal traversal) {
        super(traversal);
        this.setConsumer(traverserSet -> {
            final Random random = new Random();
            traverserSet.sort((a, b) -> random.nextBoolean() ? -1 : 1);
        });
    }
}
