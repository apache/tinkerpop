package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.ByComparatorAcceptor;
import com.tinkerpop.gremlin.process.graph.marker.Comparing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.ByComparator;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderStep<S> extends BarrierStep<S> implements Reversible, Comparing<S>, ByComparatorAcceptor<S> {

    private ByComparator<S> byComparator;

    public OrderStep(final Traversal traversal) {
        super(traversal);
        this.setConsumer(traversers -> traversers.sort((a, b) -> a.compareTo(b)));
    }

    public void setByComparator(final ByComparator<S> byComparator) {
        this.byComparator = byComparator;
        final Comparator<Traverser<S>> chainedComparator = this.byComparator.prepareTraverserComparators().stream().reduce((a, b) -> a.thenComparing(b)).get();
        this.setConsumer(traversers -> traversers.sort(chainedComparator));
    }


    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.byComparator);
    }

    @Override
    public Comparator<Traverser<S>>[] getComparators() {
        if (null == this.byComparator)
            return new Comparator[]{(a, b) -> ((Traverser) a).compareTo(((Traverser) b))};
        else {
            final List<Comparator<Traverser<S>>> comparators = this.byComparator.prepareTraverserComparators();
            return comparators.toArray(new Comparator[comparators.size()]);
        }
    }
}
