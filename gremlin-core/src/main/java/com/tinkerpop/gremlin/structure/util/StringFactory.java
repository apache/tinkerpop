package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.FunctionUtils;
import org.javatuples.Pair;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
    private static final String MP = "mp";
    private static final String L_BRACKET = "[";
    private static final String R_BRACKET = "]";
    private static final String COMMA_SPACE = ", ";
    private static final String COLON = ":";
    private static final String EMPTY_MAP = "{}";
    private static final String DOTS = "...";
    private static final String DASH = "-";
    private static final String ARROW = "->";
    private static final String EMPTY_PROPERTY = "p[empty]";
    private static final String EMPTY_META_PROPERTY = "mp[empty]";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String featuresStartWith = "supports";
    private static final int prefixLength = featuresStartWith.length();

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Vertex}.
     */
    public static String vertexString(final Vertex vertex) {
        return V + L_BRACKET + vertex.id() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Edge}.
     */
    public static String edgeString(final Edge edge) {
        final Vertex inV = edge.iterators().vertices(Direction.IN).next();
        final Vertex outV = edge.iterators().vertices(Direction.OUT).next();
        return E + L_BRACKET + edge.id() + R_BRACKET + L_BRACKET + outV.id() + DASH + edge.label() + ARROW + inV.id() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Property} or {@link com.tinkerpop.gremlin.structure.MetaProperty}.
     */
    public static String propertyString(final Property property) {
        if (property instanceof MetaProperty) {
            return property.isPresent() ? MP + L_BRACKET + property.key() + ARROW + property.value() + R_BRACKET : EMPTY_META_PROPERTY;
        } else {
            return property.isPresent() ? P + L_BRACKET + property.key() + ARROW + property.value() + R_BRACKET : EMPTY_PROPERTY;
        }
    }

    /**
     * Construct the representation for a {@link com.tinkerpop.gremlin.structure.Graph}.
     *
     * @param internalString a custom {@link String} that appends to the end of the standard representation
     */
    public static String graphString(final Graph graph, final String internalString) {
        return graph.getClass().getSimpleName().toLowerCase() + L_BRACKET + internalString + R_BRACKET;
    }

    public static String graphVariablesString(final Graph.Variables variables) {
        return "variables" + L_BRACKET + "size:" + variables.keys().size() + R_BRACKET;
    }

    public static String computeMemoryString(final Memory memory) {
        return "memory" + L_BRACKET + "size:" + memory.keys().size() + R_BRACKET;
    }

    public static String computerString(final GraphComputer graphComputer) {
        return graphComputer.getClass().getSimpleName().toLowerCase();
    }

    public static String featureString(final Graph.Features features) {
        final StringBuilder sb = new StringBuilder("FEATURES");
        final Predicate<Method> supportMethods = (m) -> m.getModifiers() == Modifier.PUBLIC && m.getName().startsWith(featuresStartWith) && !m.getName().equals(featuresStartWith);
        sb.append(LINE_SEPARATOR);

        Stream.of(Pair.with(Graph.Features.GraphFeatures.class, features.graph()),
                Pair.with(Graph.Features.VariableFeatures.class, features.graph().variables()),
                Pair.with(Graph.Features.VertexFeatures.class, features.vertex()),
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
        return FunctionUtils.wrapFunction((m) -> ">-- " + m.getName().substring(prefixLength) + ": " + m.invoke(features).toString() + LINE_SEPARATOR);
    }

    private static void printFeatureTitle(final Class<? extends Graph.Features.FeatureSet> featureClass, final StringBuilder sb) {
        sb.append("> ");
        sb.append(featureClass.getSimpleName());
        sb.append(LINE_SEPARATOR);
    }
}
