package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.structure.Element;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementValueFunction<V> implements Function<Element, V> {

    private final String propertyKey;

    public ElementValueFunction(final String propertyKey) {
        this.propertyKey = propertyKey;
    }

    @Override
    public V apply(final Element element) {
        return element.value(this.propertyKey);
    }

    public String getPropertyKey() {
        return this.propertyKey;
    }

    @Override
    public String toString() {
        return "value(" + this.propertyKey + ")";
    }
}
