package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.AnnotatedValue;
import com.tinkerpop.blueprints.Annotations;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;


/**
 * A collection of helpful methods for creating standard {@link Object#toString()} representations of graph-related
 * objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StringFactory {

    private static final String V = "v";
    private static final String E = "e";
    private static final String P = "p";
    private static final String L_BRACKET = "[";
    private static final String R_BRACKET = "]";
    private static final String COMMA_SPACE = ", ";
    private static final String COLON = ":";
    private static final String EMPTY_MAP = "{}";
    private static final String DOTS = "...";
    private static final String DASH = "-";
    private static final String ARROW = "->";
    private static final String EMPTY_PROPERTY = "p[empty]";

    /**
     * Construct the representation for a {@link Vertex}.
     */
    public static String vertexString(final Vertex vertex) {
        return V + L_BRACKET + vertex.getId() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link Edge}.
     */
    public static String edgeString(final Edge edge) {
        return E + L_BRACKET + edge.getId() + R_BRACKET + L_BRACKET + edge.getVertex(Direction.OUT).getId() + DASH + edge.getLabel() + ARROW + edge.getVertex(Direction.IN).getId() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link Property}.
     */
    public static String propertyString(final Property property) {
        return property.isPresent() ? P + L_BRACKET + property.getKey() + ARROW + property.get() + R_BRACKET : EMPTY_PROPERTY;
    }

    /**
     * Construct the representation for a {@link Graph}.
     *
     * @param internalString a custom {@link String} that appends to the end of the standard representation
     */
    public static String graphString(final Graph graph, final String internalString) {
        return graph.getClass().getSimpleName().toLowerCase() + L_BRACKET + internalString + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link AnnotatedList}.
     */
    public static String annotatedListString(final AnnotatedList annotatedList) {
        final StringBuilder builder = new StringBuilder(L_BRACKET);
        annotatedList.query().limit(2).values().forEach(v -> builder.append(v).append(COMMA_SPACE));
        if (builder.length() > 1)
            builder.append(DOTS);
        builder.append(R_BRACKET);
        return builder.toString();
    }

    /**
     * Construct the representation for a {@link AnnotatedValue}.
     */
    public static String annotatedValueString(final AnnotatedValue annotatedValue) {
        return L_BRACKET + annotatedValue.getValue() + COLON + EMPTY_MAP + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link Annotations}.
     */
    public static String annotationsString(final Annotations annotations) {
        return annotations.toString();
    }
}
