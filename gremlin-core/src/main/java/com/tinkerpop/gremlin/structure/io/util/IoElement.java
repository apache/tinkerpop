package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Element;

import java.util.Map;

/**
 * Serializable form of an {@link Element} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class IoElement {
    public Object id;
    public String label;
    public Map properties;
    public Map hiddenProperties;

    protected static <T extends IoElement, E extends Element> T from(final E element, final T ioe) {
        ioe.id = element.id();
        ioe.label = element.label();
        ioe.properties = element.values();
        ioe.hiddenProperties = element.hiddenValues();

        return ioe;
    }
}
