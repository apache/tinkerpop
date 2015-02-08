package com.tinkerpop.gremlin.process.traversal.step;

import com.tinkerpop.gremlin.structure.Element;

import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementValueComparator<V> implements Comparator<Element> {

    private final String propertyKey;
    private final Comparator<V> valueComparator;

    public ElementValueComparator(final String propertyKey, final Comparator<V> valueComparator) {
        this.propertyKey = propertyKey;
        this.valueComparator = valueComparator;
    }

    public String getPropertyKey() {
        return this.propertyKey;
    }

    public Comparator<V> getValueComparator() {
        return this.valueComparator;
    }

    @Override
    public int compare(final Element elementA, final Element elementB) {
        return this.valueComparator.compare(elementA.value(this.propertyKey), elementB.value(this.propertyKey));
    }

    @Override
    public String toString() {
        return this.propertyKey + '(' + this.valueComparator + ')';
    }
}
