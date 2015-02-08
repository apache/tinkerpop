package com.tinkerpop.gremlin.process.traversal.lambda;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ElementValueTraversal<V> extends AbstractLambdaTraversal<Element, V> {

    private final String propertyKey;
    private V value;

    public ElementValueTraversal(final String propertyKey) {
        this.propertyKey = propertyKey;
    }

    @Override
    public V next() {
        return this.value;
    }

    @Override
    public void addStart(final Traverser<Element> start) {
        this.value = start.get().value(this.propertyKey);
    }

    public String getPropertyKey() {
        return this.propertyKey;
    }

    @Override
    public String toString() {
        return "value(" + this.propertyKey + ')';
    }
}
