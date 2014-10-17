package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderStep<S> extends BarrierStep<S> implements Reversible {

    private final Comparator<Traverser<S>> comparator;

    public OrderStep(final Traversal traversal, final Comparator<Traverser<S>> comparator) {
        super(traversal);
        this.comparator = comparator;
        this.setConsumer(traverserSet -> traverserSet.sort(this.comparator));
    }

    public Comparator<Traverser<S>> getComparator() {
        return this.comparator;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.comparator);
    }
}
