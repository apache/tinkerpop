package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopProperty<V> implements Property<V>, WrappedProperty<Property<V>> {

    private final Property<V> baseProperty;
    private final Element hadoopElement;

    protected HadoopProperty(final Property<V> baseProperty, final Element hadoopElement) {
        this.baseProperty = baseProperty;
        this.hadoopElement = hadoopElement;
    }

    @Override
    public boolean isPresent() {
        return this.baseProperty.isPresent();
    }

    @Override
    public V value() {
        return this.baseProperty.value();
    }

    @Override
    public boolean isHidden() {
        return this.baseProperty.isHidden();
    }

    @Override
    public Property<V> getBaseProperty() {
        return this.baseProperty;
    }

    @Override
    public String key() {
        return this.baseProperty.key();
    }

    @Override
    public void remove() {
        this.baseProperty.remove();
    }

    @Override
    public Element element() {
        return this.hadoopElement;
    }

    @Override
    public boolean equals(final Object object) {
        return this.baseProperty.equals(object);
    }

    @Override
    public int hashCode() {
        return this.baseProperty.hashCode();
    }

    @Override
    public String toString() {
        return this.baseProperty.toString();
    }
}
