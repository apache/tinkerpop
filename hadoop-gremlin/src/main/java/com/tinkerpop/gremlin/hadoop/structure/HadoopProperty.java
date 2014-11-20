package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HadoopProperty<V> implements Property<V>, WrappedProperty<TinkerProperty<V>> {

    private final TinkerProperty<V> tinkerProperty;
    private final Element hadoopElement;

    protected HadoopProperty(final TinkerProperty<V> tinkerProperty, final Element hadoopElement) {
        this.tinkerProperty = tinkerProperty;
        this.hadoopElement = hadoopElement;
    }

    @Override
    public boolean isPresent() {
        return this.tinkerProperty.isPresent();
    }

    @Override
    public V value() {
        return this.tinkerProperty.value();
    }

    @Override
    public boolean isHidden() {
        return this.tinkerProperty.isHidden();
    }

    @Override
    public TinkerProperty<V> getBaseProperty() {
        return this.tinkerProperty;
    }

    @Override
    public String key() {
        return this.tinkerProperty.key();
    }

    @Override
    public void remove() {
        this.tinkerProperty.remove();
    }

    @Override
    public Element element() {
        return this.hadoopElement;
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return this.tinkerProperty.hashCode();
    }

    @Override
    public String toString() {
        return this.tinkerProperty.toString();
    }
}
