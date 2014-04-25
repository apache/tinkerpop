package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;

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
        ioe.properties = element.getProperties().entrySet().stream()
                .map(entry-> entry.getValue().get() instanceof AnnotatedList ?
                        Pair.<String,Object>with(entry.getKey(), IOAnnotatedList.from((AnnotatedList) entry.getValue().get())) :
                        Pair.<String,Object>with(entry.getKey(), entry.getValue().get()))
                .collect(Collectors.toMap(p -> p.getValue0(), p -> p.getValue1()));

        return ioe;
    }
}
