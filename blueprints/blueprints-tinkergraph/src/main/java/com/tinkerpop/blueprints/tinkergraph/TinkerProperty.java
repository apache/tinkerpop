package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerProperty<V> implements Property<V> {

    private Map<String, Object> annotations = new HashMap<>();
    private final Element element;
    private final String key;
    private final V value;

    protected TinkerGraphComputer.State state = TinkerGraphComputer.State.STANDARD;
    private TinkerVertexMemory vertexMemory;

    public TinkerProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    private TinkerProperty(final TinkerProperty<V> property, final TinkerGraphComputer.State state, final TinkerVertexMemory vertexMemory) {
        this(property.getElement(), property.getKey(), property.getValue());
        this.state = state;
        this.annotations = property.annotations;
        this.vertexMemory = vertexMemory;
    }

    public <E extends Element> E getElement() {
        return (E) this.element;
    }

    public String getKey() {
        return this.key;
    }

    public V getValue() {
        return this.value;
    }

    public boolean isPresent() {
        return null != this.value;
    }

    public <V> void setAnnotation(final String key, final V value) {
        if (TinkerGraphComputer.State.STANDARD == this.state) {
            this.annotations.put(key, value);
        } else if (TinkerGraphComputer.State.CENTRIC == this.state) {
            if (this.vertexMemory.isComputeKey(key))
                this.vertexMemory.setAnnotation(this, key, value);
            else
                throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        } else {
            throw GraphComputer.Exceptions.adjacentAnnotationsCanNotBeReadOrWritten();
        }
    }

    public <V> Optional<V> getAnnotation(final String key) {
        if (this.state == TinkerGraphComputer.State.STANDARD) {
            return Optional.ofNullable((V) this.annotations.get(key));
        } else if (this.state == TinkerGraphComputer.State.CENTRIC) {
            if (this.vertexMemory.isComputeKey(key))
                return this.vertexMemory.getAnnotation(this, key);
            else
                return Optional.ofNullable((V) this.annotations.get(key));
        } else {
            throw GraphComputer.Exceptions.adjacentAnnotationsCanNotBeReadOrWritten();
        }
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
