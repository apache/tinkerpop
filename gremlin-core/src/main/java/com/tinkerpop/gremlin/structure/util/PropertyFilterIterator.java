package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyFilterIterator<V> implements Iterator<Property<V>> {

    private Iterator<Property> properties;
    private boolean getHiddens;
    private Set<String> propertyKeys;
    private Property<V> upNext = null;

    public PropertyFilterIterator(final Iterator<Property> properties, final boolean getHiddens, final String... propertyKeys) {
        this.properties = properties;
        this.getHiddens = getHiddens;
        this.propertyKeys = new HashSet<>();
        Collections.addAll(this.propertyKeys, propertyKeys);
    }

    public Property<V> next() {
        if (null != this.upNext) {
            final Property<V> temp = this.upNext;
            this.upNext = null;
            return temp;
        } else {
            final Property<V> temp = this.upNext();
            if (null == temp)
                throw FastNoSuchElementException.instance();
            else
                return temp;
        }
    }

    public boolean hasNext() {
        if (this.upNext != null)
            return true;
        else {
            this.upNext = this.upNext();
            return (this.upNext != null);
        }
    }

    private Property<V> upNext() {
        while (this.properties.hasNext()) {
            final Property property = this.properties.next();
            if (this.propertyKeys.isEmpty()) {
                if (this.getHiddens) {
                    if (property.isHidden())
                        return property;
                } else {
                    if (!property.isHidden())
                        return property;
                }
            } else {
                if (this.getHiddens) {
                    if (property.isHidden() && this.propertyKeys.contains(property.key()))
                        return property;
                } else {
                    if (!property.isHidden() && this.propertyKeys.contains(property.key()))
                        return property;
                }
            }
        }
        return null;
    }
}
