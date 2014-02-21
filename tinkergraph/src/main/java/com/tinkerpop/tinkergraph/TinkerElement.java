package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.strategy.Strategy;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

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

    protected TinkerElement(final String id, final String label, final TinkerGraph graph) {
        this.graph = graph;
        this.id = id;
        this.label = label;
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
        if (TinkerGraphComputer.State.STANDARD == this.state) {
            return this.properties.getOrDefault(key, Property.empty());
        } else if (TinkerGraphComputer.State.CENTRIC == this.state) {
            if (this.vertexMemory.getComputeKeys().containsKey(key))
                return this.vertexMemory.getProperty(this, key);
            else if (this.properties.containsKey(key))
                return ((TinkerProperty) this.properties.get(key)).createClone(TinkerGraphComputer.State.CENTRIC, vertexMemory);
            else
                return Property.empty();
        } else {
            throw GraphComputer.Exceptions.adjacentElementPropertiesCanNotBeRead();
        }
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public abstract void remove();
}
