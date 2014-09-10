package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.javatuples.Pair;

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
        ioe.properties = element instanceof Vertex ?
                ((Vertex) element).propertyMap().next().entrySet().stream().map(kv -> Pair.with(kv.getKey(), kv.getValue().stream().map(IoMetaProperty::from).collect(Collectors.toList()))).collect(Collectors.toMap(p -> p.getValue0(), p-> p.getValue1())) :
                ((Edge) element).valueMap().next();
        ioe.hiddenProperties = element instanceof Vertex ? ((Vertex) element).hiddenValueMap().next() : ((Edge) element).hiddenValueMap().next();
        return ioe;
    }
}
