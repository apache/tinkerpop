package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerProperty<V> implements Property<V> {

    protected final Element element;
    protected final String key;
    protected V value;
    protected final TinkerGraph graph;

    public TinkerProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
        this.graph = ((TinkerElement) this.element).graph;
    }

    @Override
    public <E extends Element> E element() {
        return (E) this.element;
    }

    @Override
    public String key() {
        return Graph.Key.unHide(this.key);
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public boolean isHidden() {
        return Graph.Key.isHidden(this.key);
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        // todo: can't use the VertexProperty to get the hashcode or it goes StackOverflow
        return this.key.hashCode() + this.value.hashCode() + (this.element instanceof TinkerVertexProperty ? 0 : this.element.hashCode());
    }

    @Override
    public void remove() {
        ((TinkerElement) this.element).properties.remove(this.key);
        if (this.element instanceof Edge)
            this.graph.edgeIndex.remove(key, value, (TinkerEdge) this.element);
    }
}
