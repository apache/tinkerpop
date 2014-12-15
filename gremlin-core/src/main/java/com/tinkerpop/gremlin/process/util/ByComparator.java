package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ByComparator<S> {

    private final List<Object> keys = new ArrayList<>();
    private final List<Comparator> valueComparators = new ArrayList<>();

    public ByComparator(final Comparator<S> comparatorA) {
        this.valueComparators.add(comparatorA);
    }

    public ByComparator(final Comparator<S> comparatorA, Comparator<S> comparatorB) {
        this.valueComparators.add(comparatorA);
        this.valueComparators.add(comparatorB);
    }

    public <C1> ByComparator(final Object keyA, final Comparator<C1> comparatorA) {
        this.keys.add(keyA);
        this.valueComparators.add(comparatorA);
    }

    public <C1, C2> ByComparator(final Object keyA, final Comparator<C1> comparatorA, final Object keyB, final Comparator<C2> comparatorB) {
        this.keys.add(keyA);
        this.keys.add(keyB);
        this.valueComparators.add(comparatorA);
        this.valueComparators.add(comparatorB);
    }

    public <C1, C2, C3> ByComparator(final Object keyA, final Comparator<C1> comparatorA, final Object keyB, final Comparator<C2> comparatorB, final Object keyC, final Comparator<C3> comparatorC) {
        this.keys.add(keyA);
        this.keys.add(keyB);
        this.keys.add(keyC);
        this.valueComparators.add(comparatorA);
        this.valueComparators.add(comparatorB);
        this.valueComparators.add(comparatorC);
    }

    public List<Comparator<S>> prepareComparators() {
        if (this.keys.size() > 0) {
            final List<Comparator<S>> elementComparators = new ArrayList<>();
            for (int i = 0; i < this.valueComparators.size(); i++) {
                elementComparators.add(new ObjectComparator(this.keys.get(i), this.valueComparators.get(i)));
            }
            return elementComparators;
        } else {
            return (List) this.valueComparators;
        }

    }

    public List<Comparator<Traverser<S>>> prepareTraverserComparators() {
        if (this.keys.size() > 0) {
            final List<Comparator<Traverser<S>>> elementComparators = new ArrayList<>();
            for (int i = 0; i < this.valueComparators.size(); i++) {
                elementComparators.add(new TraverserComparator(this.keys.get(i), this.valueComparators.get(i)));
            }
            return elementComparators;
        } else {
            final List<Comparator<Traverser<S>>> elementComparators = new ArrayList<>();
            for (int i = 0; i < this.valueComparators.size(); i++) {
                final Comparator comparator = this.valueComparators.get(i);
                elementComparators.add((a, b) -> comparator.compare(a.get(), b.get()));
            }
            return elementComparators;
        }

    }


    public class ObjectComparator implements Comparator<S> {

        private final Object key;
        private final Comparator valueComparator;
        private final Comparator<S> finalComparator;

        public ObjectComparator(final Object key, final Comparator valueComparator) {
            this.key = key;
            this.valueComparator = valueComparator;
            if (this.key instanceof String) {
                this.finalComparator = (a, b) -> this.valueComparator.compare(((Element) a).value((String) this.key), ((Element) b).value((String) this.key));
            } else {
                this.finalComparator = (a, b) -> this.valueComparator.compare(((Function) this.key).apply(a), ((Function) this.key).apply(b));
            }
        }

        public int compare(final S first, final S second) {
            return this.finalComparator.compare(first, second);
        }
    }

    public class TraverserComparator implements Comparator<Traverser<S>> {

        private final Object key;
        private final Comparator valueComparator;
        private final Comparator<Traverser<S>> finalComparator;

        public TraverserComparator(final Object key, final Comparator valueComparator) {
            this.key = key;
            this.valueComparator = valueComparator;
            if (this.key instanceof String) {
                this.finalComparator = (a, b) -> this.valueComparator.compare(((Element) a.get()).value((String) this.key), ((Element) b.get()).value((String) this.key));
            } else {
                this.finalComparator = (a, b) -> this.valueComparator.compare(((Function) this.key).apply(a.get()), ((Function) this.key).apply(b.get()));
            }
        }

        public int compare(final Traverser<S> first, final Traverser<S> second) {
            return this.finalComparator.compare(first, second);
        }
    }
}
