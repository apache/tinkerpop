package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Comparing;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;

import java.util.Comparator;
import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ShuffleStep<S> extends BarrierStep<S> implements Comparing<S> {

    private static final Random RANDOM = new Random();
    private static final Comparator SHUFFLE_COMPARATOR = (a, b) -> RANDOM.nextBoolean() ? -1 : 1;

    public ShuffleStep(final Traversal traversal) {
        super(traversal);
        this.setConsumer(traverserSet -> traverserSet.sort(SHUFFLE_COMPARATOR));
    }

    @Override
    public Comparator<Traverser<S>>[] getComparators() {
        return new Comparator[]{SHUFFLE_COMPARATOR};
    }
}
