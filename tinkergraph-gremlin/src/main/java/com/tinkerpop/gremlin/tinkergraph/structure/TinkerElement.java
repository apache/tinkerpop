package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
abstract class TinkerElement implements Element, Serializable {

    protected Map<String, Property> properties = new HashMap<>();
    protected final Object id;
    protected final String label;
    protected final TinkerGraph graph;

    protected TinkerElement(final Object id, final String label, final TinkerGraph graph) {
        this.graph = graph;
        this.id = id;
        this.label = label;
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public Object getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public Map<String, Property> getProperties() {
        /*if (this.graph.useGraphView) {
            final HashMap<String, Property> viewProperties = new HashMap<>(this.properties);
            this.graph.graphView.computeKeys.forEach((key, keyType) -> {
                if (this.graph.graphView.getProperty(this, key).isPresent())
                    viewProperties.put(key, this.graph.graphView.getProperty(this, key));
            });
            return viewProperties;
        } else {*/
        return new HashMap<>(this.properties);
        //}
    }

    public <V> Property<V> getProperty(final String key) {
        if (this.graph.useGraphView) {
            return this.graph.graphView.getProperty(this, key);
        } else {
            return this.properties.getOrDefault(key, Property.empty());
        }
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public abstract void remove();
}
