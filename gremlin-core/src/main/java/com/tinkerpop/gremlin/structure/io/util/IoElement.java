package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
        ioe.properties = new HashMap();
        if (element instanceof Vertex) {
            ((Vertex) element).iterators().properties().forEachRemaining(p -> addMetaProperty(ioe.properties, p));
        } else
            ((Edge) element).iterators().properties().forEachRemaining(p -> ioe.properties.put(p.key(), p.value()));

        ioe.hiddenProperties = new HashMap();
        if (element instanceof Vertex)
            ((Vertex) element).iterators().hiddens().forEachRemaining(p -> addMetaProperty(ioe.hiddenProperties, p));
        else
            ((Edge) element).iterators().hiddens().forEachRemaining(p -> ioe.hiddenProperties.put(p.key(), p.value()));
        return ioe;
    }

    private static void addMetaProperty(final Map<String, List> map, final MetaProperty metaProperty) {
        List list = map.get(metaProperty.key());
        if (null == list) {
            list = new ArrayList<>();
            map.put(metaProperty.key(), list);
        }
        list.add(IoMetaProperty.from(metaProperty));

    }
}
