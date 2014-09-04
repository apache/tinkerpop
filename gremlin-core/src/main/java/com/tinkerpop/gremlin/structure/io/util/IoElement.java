package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Map;
import java.util.stream.Collectors;

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
        ioe.properties = StreamFactory.stream(element.properties()).collect(Collectors.toMap(property -> ((Property) property).key(), property -> ((Property) property).value()));
        ioe.hiddenProperties = StreamFactory.stream(element.hiddens()).collect(Collectors.toMap(property -> ((Property) property).key(), property -> ((Property) property).value()));
        return ioe;
    }
}
