package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.ComparatorSupplier;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ShuffleStep<S> extends BarrierStep<S> implements ComparatorSupplier<S> {

    private static final Random RANDOM = new Random();
    private static final Comparator SHUFFLE_COMPARATOR = new Comparator() {
        @Override
        public int compare(final Object a, final Object b) {
            return RANDOM.nextBoolean() ? -1 : 1;
        }

        @Override
        public String toString() {
            return "shuffle";
        }
    };

    public ShuffleStep(final Traversal traversal) {
        super(traversal);
        this.setConsumer(traverserSet -> traverserSet.sort(SHUFFLE_COMPARATOR));
    }

    @Override
    public List<Comparator<S>> getComparators() {
        return Arrays.asList(SHUFFLE_COMPARATOR);
    }
}
