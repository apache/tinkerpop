package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.ComparatorAcceptor;
import com.tinkerpop.gremlin.process.graph.marker.Comparing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderStep<S> extends BarrierStep<S> implements Reversible, Comparing<S>, ComparatorAcceptor<S> {

    private final List<Comparator<Traverser<S>>> comparators = new ArrayList<>();

    public OrderStep(final Traversal traversal) {
        super(traversal);
        this.setConsumer(traversers -> traversers.sort((a, b) -> a.compareTo(b)));
    }

    public void addComparator(final Comparator<S> comparator) {
        this.comparators.add(new Comparator<Traverser<S>>() {
            @Override
            public int compare(Traverser<S> traverserA, Traverser<S> traverserB) {
                return comparator.compare(traverserA.get(), traverserB.get());
            }
        });
        final Comparator<Traverser<S>> chainedComparator = this.comparators.stream().reduce((a, b) -> a.thenComparing(b)).get();
        this.setConsumer(traversers -> traversers.sort(chainedComparator));
    }


    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.comparators);
    }

    @Override
    public Comparator<Traverser<S>>[] getComparators() {
        return 0 == this.comparators.size() ?
                new Comparator[]{(a, b) -> ((Traverser) a).compareTo((Traverser) b)} :
                this.comparators.toArray(new Comparator[this.comparators.size()]);
    }
}
