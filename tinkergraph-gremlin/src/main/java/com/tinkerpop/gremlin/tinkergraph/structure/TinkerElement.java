package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class TinkerElement implements Element, Serializable {

    protected Map<String, List<? extends Property>> properties = new HashMap<>();
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

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public <V> Iterator<? extends Property<V>> hiddens(final String... propertyKeys) {
        return (Iterator) this.properties.entrySet().stream()
                .filter(entry -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, entry.getKey()) >= 0)
                .filter(entry -> Graph.Key.isHidden(entry.getKey()))
                .flatMap(entry -> entry.getValue().stream()).iterator();
        /*if (this.graph.graphView != null && this.graph.graphView.getInUse()) {
            this.graph.graphView.getComputeKeys().keySet().forEach(key -> {
                if (Graph.Key.isHidden(key)) {
                    final Property property = this.graph.graphView.getProperty(this, key);
                    if (property.isPresent()) temp.put(Graph.Key.unHide(key), property);
                }
            });
        }
        return temp.values().iterator();*/
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        return (Iterator) this.properties.entrySet().stream()
                .filter(entry -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, entry.getKey()) >= 0)
                .filter(entry -> !Graph.Key.isHidden(entry.getKey()))
                .flatMap(entry -> entry.getValue().stream()).iterator();
        /*if (this.graph.graphView != null && this.graph.graphView.getInUse()) {
            this.graph.graphView.getComputeKeys().keySet().forEach(key -> {
                if (!Graph.Key.isHidden(key)) {
                    final Property property = this.graph.graphView.getProperty(this, key);
                    if (property.isPresent()) temp.put(key, property);
                }
            });
        }
        return temp.values().iterator();*/
    }


    @Override
    public <V> Property<V> property(final String key) {
        if (this.graph.graphView != null && this.graph.graphView.getInUse()) {
            return (Property<V>) this.graph.graphView.getProperty(this, key);
        } else {
            return this.properties.containsKey(key) ? this.properties.get(key).get(0) : Property.<V>empty();
        }
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public abstract void remove();
}
