package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        if (null != this.graph.graphView && this.graph.graphView.getInUse()) {
            if (propertyKeys.length == 0) {
                final Set<String> keys = new HashSet<>();
                keys.addAll(this.properties.keySet());
                keys.addAll(this.graph.graphView.getComputeKeys().keySet());
                return (Iterator) keys.stream().filter(key -> Graph.Key.isHidden(key)).flatMap(key -> this.graph.graphView.getProperty(this, key).stream()).iterator();
            } else
                return (Iterator) Arrays.stream(propertyKeys).map(key -> Graph.Key.hide(key)).flatMap(key -> this.graph.graphView.getProperty(this, key).stream()).iterator();

        } else {
            return (Iterator) this.properties.entrySet().stream()
                    .filter(entry -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, Graph.Key.unHide(entry.getKey())) >= 0)
                    .filter(entry -> Graph.Key.isHidden(entry.getKey()))
                    .flatMap(entry -> entry.getValue().stream()).iterator();
        }
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        if (null != this.graph.graphView && this.graph.graphView.getInUse()) {
            if (propertyKeys.length == 0) {
                final Set<String> keys = new HashSet<>();
                keys.addAll(this.properties.keySet());
                keys.addAll(this.graph.graphView.getComputeKeys().keySet());
                return (Iterator) keys.stream().filter(key -> !Graph.Key.isHidden(key)).flatMap(key -> this.graph.graphView.getProperty(this, key).stream()).iterator();
            } else
                return (Iterator) Arrays.stream(propertyKeys).flatMap(key -> this.graph.graphView.getProperty(this, key).stream()).iterator();

        } else {
            return (Iterator) this.properties.entrySet().stream()
                    .filter(entry -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, entry.getKey()) >= 0)
                    .filter(entry -> !Graph.Key.isHidden(entry.getKey()))
                    .flatMap(entry -> entry.getValue().stream()).iterator();
        }
    }


    /*@Override
    public <V> Property<V> property(final String key) {
        if (this.graph.graphView != null && this.graph.graphView.getInUse()) {
            final List<Property> list = this.graph.graphView.getProperty(this, key);
            return list.size() == 0 ? Property.<V>empty() : list.get(0);
        } else {
            return this.properties.containsKey(key) ? this.properties.get(key).get(0) : Property.<V>empty();
        }
    }*/

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public abstract void remove();
}
