package com.tinkerpop.gremlin.process.traversal.step;

import com.tinkerpop.gremlin.structure.Element;

import java.util.Comparator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementFunctionComparator<V> implements Comparator<Element> {

    private final Function<Element, V> elementFunction;
    private final Comparator<V> valueComparator;

    public ElementFunctionComparator(final Function<Element, V> elementFunction, final Comparator<V> valueComparator) {
        this.elementFunction = elementFunction;
        this.valueComparator = valueComparator;
    }

    public Function<Element, V> getElementFunction() {
        return this.elementFunction;
    }

    public Comparator<V> getValueComparator() {
        return this.valueComparator;
    }

    @Override
    public int compare(final Element elementA, final Element elementB) {
        return this.valueComparator.compare(this.elementFunction.apply(elementA), this.elementFunction.apply(elementB));
    }

    @Override
    public String toString() {
        return this.elementFunction + "(" + this.valueComparator + ')';
    }
}
