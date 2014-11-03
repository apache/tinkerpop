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
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderByStep<S extends Element, C> extends BarrierStep<S> implements Reversible, Comparing<S> {

    private final Comparator<C>[] propertyValueComparators;
    private final Comparator<Traverser<S>>[] elementComparators;
    private final Comparator<Traverser<S>> chainedComparator;
    private final String propertyKey;
    private final T elementAccessor;

    public OrderByStep(final Traversal traversal, final String propertyKey, final Comparator<C>... propertyValueComparators) {
        super(traversal);
        this.propertyKey = propertyKey;
        this.elementAccessor = null;
        this.propertyValueComparators = propertyValueComparators;
        this.elementComparators = new Comparator[propertyValueComparators.length];
        for (int i = 0; i < propertyValueComparators.length; i++) {
            this.elementComparators[i] = new ElementComparator(propertyKey, propertyValueComparators[i]);
        }
        this.chainedComparator = Stream.of(this.elementComparators).reduce((a, b) -> a.thenComparing(b)).get();
        this.setConsumer(traversers -> traversers.sort(this.chainedComparator));
    }

    public OrderByStep(final Traversal traversal, final T accessor, final Comparator<C>... propertyValueComparators) {
        super(traversal);
        this.propertyKey = null;
        this.elementAccessor = accessor;
        this.propertyValueComparators = propertyValueComparators;
        this.elementComparators = new Comparator[propertyValueComparators.length];
        for (int i = 0; i < propertyValueComparators.length; i++) {
            this.elementComparators[i] = new ElementComparator(accessor, propertyValueComparators[i]);
        }
        this.chainedComparator = Stream.of(this.elementComparators).reduce((a, b) -> a.thenComparing(b)).get();
        this.setConsumer(traversers -> traversers.sort(this.chainedComparator));
    }

    public Optional<String> getPropertyKey() {
        return Optional.ofNullable(this.propertyKey);
    }

    public Optional<T> getElementAccessor() {
        return Optional.ofNullable(this.elementAccessor);
    }

    public boolean usesPropertyKey() {
        return null == this.elementAccessor;
    }

    @Override
    public Comparator<Traverser<S>>[] getComparators() {
        return this.elementComparators;
    }

    public Comparator<C>[] getPropertyValueComparators() {
        return this.propertyValueComparators;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, null == this.propertyKey ? this.elementAccessor : this.propertyKey);
    }

    public class ElementComparator implements Comparator<Traverser<S>> {

        private final String propertyKey;
        private final T accessor;
        private final Comparator<C> propertyValueComparator;
        private final Comparator<Traverser<S>> comparator;

        public ElementComparator(final String propertyKey, final Comparator<C> propertyValueComparator) {
            this.propertyKey = propertyKey;
            this.accessor = null;
            this.propertyValueComparator = propertyValueComparator;
            this.comparator = (a, b) -> this.propertyValueComparator.compare(a.get().<C>value(this.propertyKey), b.get().<C>value(this.propertyKey));
        }

        public ElementComparator(final T accessor, final Comparator<C> propertyValueComparator) {
            this.propertyKey = null;
            this.accessor = accessor;
            this.propertyValueComparator = propertyValueComparator;
            switch (this.accessor) {
                case id:
                    this.comparator = (a, b) -> this.propertyValueComparator.compare((C) a.get().id(), (C) b.get().id());
                    break;
                case label:
                    this.comparator = (a, b) -> this.propertyValueComparator.compare((C) a.get().label(), (C) b.get().label());
                    break;
                case key:
                    this.comparator = (a, b) -> this.propertyValueComparator.compare((C) ((VertexProperty) a.get()).key(), (C) ((VertexProperty) b.get()).key());
                    break;
                case value:
                    this.comparator = (a, b) -> this.propertyValueComparator.compare((C) ((VertexProperty) a.get()).value(), (C) ((VertexProperty) b.get()).value());
                    break;
                default:
                    throw new IllegalArgumentException("The provided token is unknown: " + accessor.name());
            }
        }

        public int compare(final Traverser<S> first, final Traverser<S> second) {
            return this.comparator.compare(first, second);
        }
    }
}
