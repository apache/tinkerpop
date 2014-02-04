package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Strategy;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
abstract class TinkerElement implements Element, Serializable {

    protected Map<String, Property> properties = new HashMap<>();
    protected final String id;
    protected final String label;
    protected final TinkerGraph graph;

    protected String centricId;
    protected TinkerGraphComputer.State state = TinkerGraphComputer.State.STANDARD;
    protected TinkerVertexMemory vertexMemory;

    private transient final Strategy.Context<Element> elementStrategyContext;

    protected TinkerElement(final String id, final String label, final TinkerGraph graph) {
        this.graph = graph;
        this.id = id;
        this.label = label;
        this.elementStrategyContext = new Strategy.Context<Element>(this.graph, this);
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public String getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public Map<String, Property> getProperties() {
        return new HashMap<>(this.properties);
    }

    public <V> Property<V> getProperty(final String key) {
        return this.graph.strategy.compose(s -> s.<V>getElementGetProperty(elementStrategyContext), k -> {
            if (TinkerGraphComputer.State.STANDARD == this.state) {
                return this.properties.getOrDefault(k, Property.empty());
            } else if (TinkerGraphComputer.State.CENTRIC == this.state) {
                if (this.vertexMemory.getComputeKeys().containsKey(k))
                    return this.vertexMemory.getProperty(this, k);
                else if (this.properties.containsKey(key))
                    return ((TinkerProperty) this.properties.get(k)).createClone(TinkerGraphComputer.State.CENTRIC, vertexMemory);
                else
                    return Property.empty();
            } else {
                throw GraphComputer.Exceptions.adjacentElementPropertiesCanNotBeRead();
            }
        }).apply(key);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public abstract void remove();
}
