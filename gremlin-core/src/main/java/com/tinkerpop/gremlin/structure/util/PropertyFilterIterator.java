package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyFilterIterator<V> implements Iterator<Property<V>> {

    private Iterator<Property> properties;
    private boolean getHiddens;
    private String[] propertyKeys;
    private Property<V> upNext = null;

    public PropertyFilterIterator(final Iterator<Property> properties, final boolean getHiddens, final String... propertyKeys) {
        this.properties = properties;
        this.getHiddens = getHiddens;
        this.propertyKeys = propertyKeys;
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
            if (this.propertyKeys.length == 0) {
                if (this.getHiddens) {
                    if (property.isHidden())
                        return property;
                } else {
                    if (!property.isHidden())
                        return property;
                }
            } else {
                if (this.getHiddens) {
                    if (property.isHidden() && Stream.of(this.propertyKeys).filter(key -> key.equals(property.key())).findAny().isPresent())
                        return property;
                } else {
                    if (!property.isHidden() && Stream.of(this.propertyKeys).filter(key -> key.equals(property.key())).findAny().isPresent())
                        return property;
                }
            }
        }
        return null;
    }
}
