package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Comparing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.util.BarrierStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderByStep<S extends Element, C> extends BarrierStep<S> implements Reversible, Comparing<S> {

    private final Comparator<C> elementValueComparator;
    private final Comparator<Traverser<S>> comparator;
    private final String elementKey;

    public OrderByStep(final Traversal traversal, final String elementKey, final Comparator<C> elementValueComparator) {
        super(traversal);
        this.elementKey = elementKey;
        this.elementValueComparator = elementValueComparator;
        this.comparator = (a, b) -> this.elementValueComparator.compare(a.get().<C>value(this.elementKey), b.get().<C>value(this.elementKey));
        this.setConsumer(traversers -> traversers.sort(this.comparator));
    }

    public OrderByStep(final Traversal traversal, final T accessor, final Comparator<C> elementValueComparator) {
        super(traversal);
        this.elementKey = accessor.getAccessor();
        this.elementValueComparator = elementValueComparator;
        switch (accessor) {
            case id:
                this.comparator = (a, b) -> this.elementValueComparator.compare((C) a.get().id(), (C) b.get().id());
                break;
            case label:
                this.comparator = (a, b) -> this.elementValueComparator.compare((C) a.get().label(), (C) b.get().label());
                break;
            case key:
                this.comparator = (a, b) -> this.elementValueComparator.compare((C) ((VertexProperty) a.get()).key(), (C) ((VertexProperty) b.get()).key());
                break;
            case value:
                this.comparator = (a, b) -> this.elementValueComparator.compare((C) ((VertexProperty) a.get()).value(), (C) ((VertexProperty) b.get()).value());
                break;
            default:
                throw new IllegalArgumentException("The provided token is unknown: " + accessor.name());
        }
        this.setConsumer(traversers -> traversers.sort(this.comparator));
    }

    public String getElementKey() {
        return this.elementKey;
    }

    public T getElementAccessor() {
        return T.fromString(this.elementKey);
    }

    @Override
    public Comparator<Traverser<S>> getComparator() {
        return this.comparator;
    }

    public Comparator<C> getElementValueComparator() {
        return this.elementValueComparator;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.elementKey, this.comparator);
    }
}
