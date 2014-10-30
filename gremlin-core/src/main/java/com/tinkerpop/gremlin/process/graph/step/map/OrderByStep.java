package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Comparing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;

import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderByStep<S extends Element, C> extends BarrierStep<S> implements Reversible, Comparing<S> {

    private final Comparator<Traverser<S>> comparator;
    private final String elementKey;

    public OrderByStep(final Traversal traversal, final String elementKey, final Comparator<C> elementValueComparator) {
        super(traversal);
        this.elementKey = elementKey;
        this.comparator = (a, b) -> elementValueComparator.compare(a.get().<C>value(this.elementKey), b.get().<C>value(this.elementKey));
        this.setConsumer(traversers -> traversers.sort(this.comparator));
    }

    public String getElementKey() {
        return this.elementKey;
    }

    @Override
    public Comparator<Traverser<S>> getComparator() {
        return this.comparator;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.elementKey, this.comparator);
    }
}
