package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.AnnotatedList;
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

    public static final String V = "v";
    public static final String E = "e";
    public static final String P = "p";
    public static final String L_BRACKET = "[";
    public static final String R_BRACKET = "]";
    public static final String COMMA_SPACE = ", ";
    public static final String COLON = ":";
    public static final String EMPTY_MAP = "{}";
    public static final String DOTS = "...";
    public static final String DASH = "-";
    public static final String ARROW = "->";
    public static final String EMPTY_PROPERTY = "p[empty]";

    public static final String VALUE = "value";

    public static String vertexString(final Vertex vertex) {
        return V + L_BRACKET + vertex.getId() + R_BRACKET;
    }

    public static String edgeString(final Edge edge) {
        return E + L_BRACKET + edge.getId() + R_BRACKET + L_BRACKET + edge.getVertex(Direction.OUT).getId() + DASH + edge.getLabel() + ARROW + edge.getVertex(Direction.IN).getId() + R_BRACKET;
    }

    public static String propertyString(final Property property) {
        return P + L_BRACKET + property.getKey() + ARROW + property.get() + R_BRACKET;
    }

    public static String graphString(final Graph graph, final String internalString) {
        return graph.getClass().getSimpleName().toLowerCase() + L_BRACKET + internalString + R_BRACKET;
    }

    public static String annotatedListString(final AnnotatedList annotatedList) {
        final StringBuilder builder = new StringBuilder(L_BRACKET);
        annotatedList.query().limit(2).values().forEach(v -> builder.append(v).append(COMMA_SPACE));
        if (builder.length() > 1)
            builder.append(DOTS);
        builder.append(R_BRACKET);
        return builder.toString();
    }

    public static String annotatedValueString(final AnnotatedList.AnnotatedValue annotatedValue) {
        return L_BRACKET + annotatedValue.getValue() + COLON + EMPTY_MAP + R_BRACKET;
    }
}
