package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.strategy.Strategy;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerProperty<V> implements Property<V>, Serializable {

    private final Element element;
    private final String key;
    private final TinkerGraph graph;
    private V value;

    protected TinkerGraphComputer.State state = TinkerGraphComputer.State.STANDARD;

    private transient final Strategy.Context<Property<V>> strategyContext;


    public TinkerProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
        this.graph = ((TinkerElement) element).graph;
        this.strategyContext = new Strategy.Context<Property<V>>(this.graph, this);
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
            this.graph.vertexIndex.remove(key, value, (TinkerVertex) this.element);
        else
            this.graph.edgeIndex.remove(key, value, (TinkerEdge) this.element);
    }
}
