package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
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

    public TinkerProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
        this.graph = ((TinkerElement) element).graph;
    }

    public <E extends Element> E getElement() {
        return (E) this.element;
    }

    public String key() {
        return ElementHelper.removeHiddenPrefix(this.key);
    }

    public V value() {
        return this.value;
    }

    public boolean isPresent() {
        return null != this.value;
    }

    public boolean isHidden() {
       return this.key.startsWith(Graph.Key.HIDDEN_PREFIX);
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
