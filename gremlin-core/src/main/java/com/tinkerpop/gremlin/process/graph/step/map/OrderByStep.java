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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderByStep<S extends Element> extends BarrierStep<S> implements Reversible, Comparing<S> {


    private final List<Object> keys = new ArrayList<>();
    private final List<Comparator> valueComparators = new ArrayList<>();
    private final List<Comparator<Traverser<S>>> elementComparators = new ArrayList<>();

    public OrderByStep(final Traversal traversal, final Object key, final Comparator valueComparator) {
        super(traversal);
        this.keys.add(key);
        this.valueComparators.add(valueComparator);
        this.prepareComparators();
    }

    public OrderByStep(final Traversal traversal, final Object keyA, final Comparator valueComparatorA, final Object keyB, final Comparator valueComparatorB) {
        super(traversal);
        this.keys.add(keyA);
        this.keys.add(keyB);
        this.valueComparators.add(valueComparatorA);
        this.valueComparators.add(valueComparatorB);
        this.prepareComparators();
    }

    public OrderByStep(final Traversal traversal, final Object keyA, final Comparator valueComparatorA, final Object keyB, final Comparator valueComparatorB, final Object keyC, final Comparator valueComparatorC) {
        super(traversal);
        this.keys.add(keyA);
        this.keys.add(keyB);
        this.keys.add(keyC);
        this.valueComparators.add(valueComparatorA);
        this.valueComparators.add(valueComparatorB);
        this.valueComparators.add(valueComparatorC);
        this.prepareComparators();
    }

    private final void prepareComparators() {
        for (int i = 0; i < this.valueComparators.size(); i++) {
            this.elementComparators.add(new ElementComparator(this.keys.get(i), this.valueComparators.get(i)));
        }
        final Comparator<Traverser<S>> chainedComparator = this.elementComparators.stream().reduce((a, b) -> a.thenComparing(b)).get();
        this.setConsumer(traversers -> traversers.sort(chainedComparator));
    }

    public List<Object> getKeys() {
        return this.keys;
    }

    @Override
    public Comparator<Traverser<S>>[] getComparators() {
        return this.elementComparators.toArray(new Comparator[this.elementComparators.size()]);
    }

    public List<Comparator> getValueComparators() {
        return this.valueComparators;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.keys);
    }

    public class ElementComparator implements Comparator<Traverser<S>> {

        private final Object key;
        private final Comparator valueComparator;
        private final Comparator<Traverser<S>> finalComparator;

        public ElementComparator(final Object key, final Comparator valueComparator) {
            this.key = key;
            this.valueComparator = valueComparator;
            if (T.id.equals(this.key)) {
                this.finalComparator = (a, b) -> this.valueComparator.compare(a.get().id(), b.get().id());
            } else if (T.label.equals(this.key)) {
                this.finalComparator = (a, b) -> this.valueComparator.compare(a.get().label(), b.get().label());
            } else if (T.key.equals(this.key)) {
                this.finalComparator = (a, b) -> this.valueComparator.compare(((VertexProperty) a.get()).key(), ((VertexProperty) b.get()).key());
            } else if (T.value.equals(this.key)) {
                this.finalComparator = (a, b) -> this.valueComparator.compare(((VertexProperty) a.get()).value(), ((VertexProperty) b.get()).value());
            } else {
                this.finalComparator = (a, b) -> this.valueComparator.compare(a.get().value(this.key.toString()), b.get().value(this.key.toString()));
            }
        }

        public int compare(final Traverser<S> first, final Traverser<S> second) {
            return this.finalComparator.compare(first, second);
        }
    }
}
