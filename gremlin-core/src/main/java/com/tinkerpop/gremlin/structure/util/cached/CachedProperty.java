package com.tinkerpop.gremlin.structure.util.cached;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CachedProperty<V> implements Property, Serializable {

    private String key;
    private V value;
    private CachedElement element;
    private int hashCode;

    public CachedProperty(final String key, final V val, final CachedElement element) {
        if (null == key) throw Graph.Exceptions.argumentCanNotBeNull("key");
        if (null == val) throw Graph.Exceptions.argumentCanNotBeNull("val");
        if (null == element) throw Graph.Exceptions.argumentCanNotBeNull("element");

        this.key = key;
        this.value = val;
        this.element = element;
    }

    public CachedProperty(final Property property) {
        if (null == property) throw Graph.Exceptions.argumentCanNotBeNull("property");

        this.key = property.getKey();
        this.value = (V) property.get();
        this.hashCode = property.hashCode();
        final Element ele = property.getElement();
        if (ele instanceof Vertex)
            this.element = new CachedVertex((Vertex) ele);
        else
            this.element = new CachedEdge((Edge) ele);
    }

    public boolean isPresent() {
        return true;
    }

    public String getKey() {
        return this.key;
    }

    public V get() {
        return this.value;
    }

    public Element getElement() {
        return this.element;
    }

    public void remove() {
        throw new UnsupportedOperationException("Cached properties are readonly: " + this.toString());
    }

    public boolean isHidden() {
        return false; //TODO:
    }

    public void hidden(final boolean makeHidden) {

    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.hashCode;
    }
}
