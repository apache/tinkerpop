package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Comparator;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Comparators {
    public static final Comparator<Element> ELEMENT_COMPARATOR = Comparator.comparing(e -> e.id().toString(), String.CASE_INSENSITIVE_ORDER);
    public static final Comparator<Vertex> VERTEX_COMPARATOR = Comparator.comparing(e -> e.id().toString(), String.CASE_INSENSITIVE_ORDER);
    public static final Comparator<Edge> EDGE_COMPARATOR = Comparator.comparing(e -> e.id().toString(), String.CASE_INSENSITIVE_ORDER);
    public static final Comparator<Property> PROPERTY_COMPARATOR = Comparator.comparing(Property::key, String.CASE_INSENSITIVE_ORDER);
    public static final Comparator<Map.Entry<String, Property>> PROPERTY_ENTRY_COMPARATOR = Comparator.comparing(Map.Entry::getKey, String.CASE_INSENSITIVE_ORDER);
    public static final Comparator<Map.Entry<String, Object>> OBJECT_ENTRY_COMPARATOR = Comparator.comparing(Map.Entry::getKey, String.CASE_INSENSITIVE_ORDER);
}
