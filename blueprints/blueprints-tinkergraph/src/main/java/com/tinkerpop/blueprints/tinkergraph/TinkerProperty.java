package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.util.DefaultAnnotatedList;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerProperty<V> implements Property<V> {

    private final Element element;
    private final String key;
    private V value;

    protected TinkerGraphComputer.State state = TinkerGraphComputer.State.STANDARD;

    public TinkerProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    private TinkerProperty(final TinkerProperty<V> property, final TinkerGraphComputer.State state, final TinkerVertexMemory vertexMemory) {
        this(property.getElement(), property.getKey(), property.get());
        this.state = state;
    }

    public <E extends Element> E getElement() {
        return (E) this.element;
    }

    public String getKey() {
        return this.key;
    }

    public V get() {
        return this.value;
    }

    public boolean isPresent() {
        return null != this.value;
    }

    public TinkerProperty<V> createClone(final TinkerGraphComputer.State state, final TinkerVertexMemory vertexMemory) {
        return new TinkerProperty<V>(this, state, vertexMemory) {
            @Override
            public void remove() {
                throw new UnsupportedOperationException("Property removal is not supported");
            }
        };
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.key.hashCode() + this.value.hashCode() + this.element.hashCode();
    }

    public void remove() {
        ((TinkerElement) this.element).properties.remove(key);
        if (this.element instanceof Vertex)
            ((TinkerVertex) this.element).graph.vertexIndex.remove(key, value, (TinkerVertex) this.element);
        else
            ((TinkerEdge) this.element).graph.edgeIndex.remove(key, value, (TinkerEdge) this.element);
    }
}
