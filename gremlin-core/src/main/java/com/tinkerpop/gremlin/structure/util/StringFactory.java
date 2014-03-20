package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.javatuples.Pair;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;


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
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String featuresStartWith = "supports";
    private static final int prefixLength = featuresStartWith.length();

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Vertex}.
     */
    public static String vertexString(final Vertex vertex) {
        return V + L_BRACKET + vertex.getId() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Edge}.
     */
    public static String edgeString(final Edge edge) {
        return E + L_BRACKET + edge.getId() + R_BRACKET + L_BRACKET + edge.getVertex(Direction.OUT).getId() + DASH + edge.getLabel() + ARROW + edge.getVertex(Direction.IN).getId() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Property}.
     */
    public static String propertyString(final Property property) {
        return property.isPresent() ? P + L_BRACKET + property.getKey() + ARROW + property.get() + R_BRACKET : EMPTY_PROPERTY;
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Graph}.
     *
     * @param internalString a custom {@link String} that appends to the end of the standard representation
     */
    public static String graphString(final Graph graph, final String internalString) {
        return graph.getClass().getSimpleName().toLowerCase() + L_BRACKET + internalString + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.AnnotatedList}.
     */
    public static String annotatedListString(final AnnotatedList<?> annotatedList) {
        final StringBuilder builder = new StringBuilder(L_BRACKET);
        annotatedList.values().range(0, 1).forEach(v -> builder.append(v).append(COMMA_SPACE));
        if (builder.length() > 1)
            builder.append(DOTS);
        builder.append(R_BRACKET);
        return builder.toString();
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.AnnotatedValue}.
     */
    public static String annotatedValueString(final AnnotatedValue<?> annotatedValue) {
        final Map<String, Object> annotations = new HashMap<>();
        annotatedValue.getAnnotationKeys().forEach(key -> annotations.put(key, annotatedValue.getAnnotation(key).get()));
        return L_BRACKET + annotatedValue.getValue() + COLON + annotations.toString() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Graph.Memory}.
     */
    public static String memoryString(final Graph.Memory memory) {
        // todo: should this be owned by the implementation...it won't be consistent
        return memory.toString();
    }

    public static String featureString(final Graph.Features features) {
        final StringBuilder sb = new StringBuilder("FEATURES");
        final Predicate<Method> supportMethods = (m) -> m.getModifiers() == Modifier.PUBLIC && m.getName().startsWith(featuresStartWith) && !m.getName().equals(featuresStartWith);
        sb.append(LINE_SEPARATOR);

        Stream.of(Pair.with(Graph.Features.GraphFeatures.class, features.graph()),
                Pair.with(Graph.Features.MemoryFeatures.class, features.graph().memory()),
                Pair.with(Graph.Features.VertexFeatures.class, features.vertex()),
                Pair.with(Graph.Features.VertexAnnotationFeatures.class, features.vertex().annotations()),
                Pair.with(Graph.Features.VertexPropertyFeatures.class, features.vertex().properties()),
                Pair.with(Graph.Features.EdgeFeatures.class, features.edge()),
                Pair.with(Graph.Features.EdgePropertyFeatures.class, features.edge().properties())).forEach(p -> {
            printFeatureTitle(p.getValue0(), sb);
            Stream.of(p.getValue0().getMethods())
                    .filter(supportMethods)
                    .map(createTransform(p.getValue1()))
                    .forEach(sb::append);
        });

        return sb.toString();
    }

    private static Function<Method, String> createTransform(final Graph.Features.FeatureSet features) {
        return FunctionUtils.wrap((m) ->  ">-- " + m.getName().substring(prefixLength) + ": " + m.invoke(features, null).toString() + LINE_SEPARATOR);
    }

    private static void printFeatureTitle(final Class<? extends Graph.Features.FeatureSet> featureClass, final StringBuilder sb) {
        sb.append("> ");
        sb.append(featureClass.getSimpleName());
        sb.append(LINE_SEPARATOR);
    }
}
