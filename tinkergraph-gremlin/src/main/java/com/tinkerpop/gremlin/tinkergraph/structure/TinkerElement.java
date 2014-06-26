package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
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

    public Object id() {
        return this.id;
    }

    public String label() {
        return this.label;
    }

    public Map<String, Property> hiddens() {
        final Map<String, Property> temp = new HashMap<>();
        this.properties.forEach((key, property) -> {
            if (Graph.Key.isHidden(key))
                temp.put(Graph.Key.unHide(key), property);
        });
        if (this.graph.useGraphView) {
            this.graph.graphView.computeKeys.keySet().forEach(key -> {
                if (Graph.Key.isHidden(key)) {
                    final Property property = this.graph.graphView.getProperty(this, key);
                    if (property.isPresent()) temp.put(Graph.Key.unHide(key), property);
                }
            });
        }
        return temp;
    }

    public Map<String, Property> properties() {
        final Map<String, Property> temp = new HashMap<>();
        this.properties.forEach((key, property) -> {
            if (!Graph.Key.isHidden(key))
                temp.put(key, property);
        });
        if (this.graph.useGraphView) {
            this.graph.graphView.computeKeys.keySet().forEach(key -> {
                if (!Graph.Key.isHidden(key)) {
                    final Property property = this.graph.graphView.getProperty(this, key);
                    if (property.isPresent()) temp.put(key, property);
                }
            });
        }
        return temp;
    }


    public <V> Property<V> property(final String key) {
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
