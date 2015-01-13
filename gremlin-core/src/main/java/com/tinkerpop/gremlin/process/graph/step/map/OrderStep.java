package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.ComparatorHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderStep<S> extends BarrierStep<S> implements Reversible, ComparatorHolder<S> {

    private final List<Comparator<S>> comparators = new ArrayList<>();

    public OrderStep(final Traversal traversal) {
        super(traversal);
        this.setConsumer(traversers -> traversers.sort((a, b) -> a.compareTo(b)));
    }

    @Override
    public void addComparator(final Comparator<S> comparator) {
        this.comparators.add(comparator);
        final Comparator<Traverser<S>> chainedComparator = this.comparators.stream().map(c -> (Comparator<Traverser<S>>) new Comparator<Traverser<S>>() {
            @Override
            public int compare(final Traverser<S> traverserA, final Traverser<S> traverserB) {
                return c.compare(traverserA.get(), traverserB.get());
            }
        }).reduce((a, b) -> a.thenComparing(b)).get();
        this.setConsumer(traversers -> traversers.sort(chainedComparator));
    }

    @Override
    public List<Comparator<S>> getComparators() {
        return this.comparators;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.comparators);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
