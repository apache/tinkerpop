package com.tinkerpop.tinkergraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;

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
        // The first argument to compose() gets the GraphStrategy to use and provides it the Context of the remove
        // call. The second argument to compose() is the TinkerGraph implementation of remove as a lambda where
        // the argument refer to the arguments to remove. Note that arguments passes through the GraphStrategy
        // implementations first so at this point the values within them may not be the same as they originally were.
        // The composed function must then be applied with the arguments originally passed to remove.
        this.graph.strategy().compose(
                s -> s.getRemovePropertyStrategy(strategyContext),
                () -> {
                    ((TinkerElement) this.element).properties.remove(key);
                    if (this.element instanceof Vertex)
                        this.graph.vertexIndex.remove(key, value, (TinkerVertex) this.element);
                    else
                        this.graph.edgeIndex.remove(key, value, (TinkerEdge) this.element);
                    return null;
                }).get();
    }
}
