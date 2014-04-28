package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Element;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serializable form of an {@link Element} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class IOElement {
    public Object id;
    public String label;
    public Map properties;

    protected static <T extends IOElement, E extends Element> T from(final E element, final T ioe) {
        ioe.id = element.getId();
        ioe.label = element.getLabel();

        // get the value out of a Property.
        ioe.properties = element.getProperties().entrySet().stream().collect(Collectors.toMap(e->e.getKey(), e-> e.getValue().get()));
        return ioe;
    }
}
