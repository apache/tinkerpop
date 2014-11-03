package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Comparing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderStep<S> extends BarrierStep<S> implements Reversible, Comparing<S> {

    private final Comparator<Traverser<S>>[] comparators;
    private final Comparator<Traverser<S>> chainedComparator;

    public OrderStep(final Traversal traversal, final Comparator<Traverser<S>>... comparators) {
        super(traversal);
        this.comparators = comparators;
        this.chainedComparator = Stream.of(this.comparators).reduce((a, b) -> a.thenComparing(b)).get();
        this.setConsumer(traverserSet -> traverserSet.sort(this.chainedComparator));
    }

    @Override
    public Comparator<Traverser<S>>[] getComparators() {
        return this.comparators;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.comparators);
    }
}
