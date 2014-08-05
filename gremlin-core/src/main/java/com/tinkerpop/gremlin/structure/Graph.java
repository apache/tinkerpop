package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.javatuples.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link Graph} is a container object for a collection of {@link Vertex}, {@link Edge}, and {@link Property}
 * objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Graph extends AutoCloseable {

    /**
     * Key is a helper class for manipulating keys wherever they may be (e.g. properties, memory, sideEffects, etc.)
     */
    public class Key {

        /**
         * A low probability prefix to denote that a key is hidden.
         */
        private static final String HIDDEN_PREFIX = "%&%";

        /**
         * Turn the provided key into a hidden key. If the key is already hidden, return key.
         *
         * @param key The key to hide
         * @return The hidden key
         */
        public static String hide(final String key) {
            return isHidden(key) ? key : HIDDEN_PREFIX.concat(key);
        }

        /**
         * Turn the provided hidden key into an unhidden key. If the key is not hidden, return key.
         *
         * @param key The hidden key
         * @return The unhidden representation of the key
         */
        public static String unHide(final String key) {
            return isHidden(key) ? key.substring(HIDDEN_PREFIX.length()) : key;
        }

        /**
         * Determines whether the provided key is hidden or not.
         *
         * @param key The key to check for hidden status
         * @return Whether the provided key is hidden or not
         */
        public static boolean isHidden(final String key) {
            return key.startsWith(HIDDEN_PREFIX);
        }
    }

    /**
     * Add a {@link Vertex} to the graph given an optional series of key/value pairs.  These key/values
     * must be provided in an even number where the odd numbered arguments are {@link String} property keys and the
     * even numbered arguments are the related property values.  Hidden properties can be set by specifying
     * the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hide}.
     *
     * @param keyValues The key/value pairs to turn into vertex properties
     * @return The newly created vertex
     */
    public Vertex addVertex(final Object... keyValues);

    /**
     * Get a {@link Vertex} given its unique identifier.
     *
     * @param id The unique identifier of the vertex to locate
     * @throws NoSuchElementException if the vertex is not found.
     */
    public default Vertex v(final Object id) throws NoSuchElementException {
        if (null == id) throw Graph.Exceptions.elementNotFound(Vertex.class, null);
        return (Vertex) this.V().has(Element.ID, id).next();
    }

    /**
     * Get a {@link Edge} given its unique identifier.
     *
     * @param id The unique identifier of the edge to locate
     * @throws NoSuchElementException if the edge is not found.
     */
    public default Edge e(final Object id) throws NoSuchElementException {
        if (null == id) throw Graph.Exceptions.elementNotFound(Edge.class, null);
        return (Edge) this.E().has(Element.ID, id).next();
    }

    /**
     * Starts a {@link GraphTraversal} over all vertices in the graph.
     */
    public GraphTraversal<Vertex, Vertex> V();

    /**
     * Starts a {@link GraphTraversal} over all edges in the graph.
     */
    public GraphTraversal<Edge, Edge> E();

    /**
     * Constructs a new domain specific {@link Traversal} for this graph.
     *
     * @param traversalClass the domain specific {@link Traversal} bound to this graph
     */
    public default <T extends Traversal<S, S>, S> T of(final Class<T> traversalClass) {
        try {
            return (T) traversalClass.getMethod(Traversal.OF, Graph.class).invoke(null, this);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * Constructs a new {@link GraphTraversal} for this graph.
     *
     * @param <S> The start class of the GraphTraversal
     * @return The newly constructed GraphTraversal bound to this graph
     */
    public default <S> GraphTraversal<S, S> of() {
        return GraphTraversal.of(this);
    }

    /**
     * Create an OLAP {@link GraphComputer} to execute a vertex program over this graph.
     * If the graph does not support graph computer then an {@link java.lang.UnsupportedOperationException} is thrown.
     * The provided arguments can be of either length 0 or 1. A graph can support multiple graph computers.
     *
     * @param graphComputerClass The graph computer class to use (if no argument, then a default is selected by the graph)
     * @param <C>                The class of the graph computer
     * @return A graph computer for processing this graph
     */
    public <C extends GraphComputer> C compute(final Class<C>... graphComputerClass);

    /**
     * Configure and control the transactions for those graphs that support this feature.
     */
    public Transaction tx();

    /**
     * A collection of global {@link Variables} associated with the graph.
     * Variables are used for storing metadata about the graph.
     *
     * @return The variables associated with this graph
     */
    public Variables variables();

    /**
     * Graph variables are a set of key/value pairs associated with the graph.
     * The keys are String and the values are Objects.
     */
    public interface Variables {

        public Set<String> keys();

        public <R> Optional<R> get(final String key);

        public void set(final String key, Object value);

        public void remove(final String key);

        public default Map<String, Object> asMap() {
            final Map<String, Object> map = keys().stream()
                    .map(key -> Pair.with(key, get(key).get()))
                    .collect(Collectors.toMap(Pair::getValue0, Pair::getValue1));
            return Collections.unmodifiableMap(map);
        }

        public static class Exceptions {

            public static IllegalArgumentException variableKeyCanNotBeEmpty() {
                return new IllegalArgumentException("Graph variable key can not be the empty string");
            }

            public static IllegalArgumentException variableKeyCanNotBeNull() {
                return new IllegalArgumentException("Graph variable key can not be null");
            }

            public static IllegalArgumentException variableValueCanNotBeNull() {
                return new IllegalArgumentException("Graph variable value can not be null");
            }

            public static UnsupportedOperationException dataTypeOfVariableValueNotSupported(final Object val) {
                return new UnsupportedOperationException(String.format("Graph variable value [%s] is of type %s is not supported", val, val.getClass()));
            }
        }

    }

    /**
     * Gets the {@link Features} exposed by the underlying {@code Graph} implementation.
     */
    public default Features getFeatures() {
        return new Features() {
        };
    }

    /**
     * An interface that represents the capabilities of a {@code Graph} implementation.  By default all methods
     * of features return {@code true} and it is up to implementers to disable feature they don't support.  Users
     * should check features prior to using various functions of TinkerPop to help ensure code portability
     * across implementations.  For example, a common usage would be to check if a graph supports transactions prior
     * to calling the commit method on {@link #tx()}.
     */
    public interface Features {

        /**
         * Gets the features related to "graph" operation.
         */
        public default GraphFeatures graph() {
            return new GraphFeatures() {
            };
        }

        /**
         * Gets the features related to "vertex" operation.
         */
        public default VertexFeatures vertex() {
            return new VertexFeatures() {
            };
        }

        /**
         * Gets the features related to "edge" operation.
         */
        public default EdgeFeatures edge() {
            return new EdgeFeatures() {
            };
        }

        /**
         * Features specific to a operations of a "graph".
         */
        public interface GraphFeatures extends FeatureSet {
            public static final String FEATURE_COMPUTER = "Computer";
            public static final String FEATURE_TRANSACTIONS = "Transactions";
            public static final String FEATURE_PERSISTENCE = "Persistence";
            public static final String FEATURE_THREADED_TRANSACTIONS = "ThreadedTransactions";
            public static final String FEATURE_FULLY_ISOLATED_TRANSACTIONS = "FullyIsolatedTransactions";

            /**
             * Determines if the {@code Graph} implementation supports
             * {@link com.tinkerpop.gremlin.process.computer.GraphComputer} based processing.
             */
            @FeatureDescriptor(name = FEATURE_COMPUTER)
            public default boolean supportsComputer() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation supports persisting it's contents natively to disk.
             * This feature does not refer to every graph's ability to write to disk via the Gremlin IO packages
             * (.e.g. GraphML), unless the graph natively persists to disk via those options somehow.  For example,
             * TinkerGraph does not support this feature as it is a pure in-memory graph.
             */
            @FeatureDescriptor(name = FEATURE_PERSISTENCE)
            public default boolean supportsPersistence() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementations supports transactions.
             */
            @FeatureDescriptor(name = FEATURE_TRANSACTIONS)
            public default boolean supportsTransactions() {
                return true;
            }

            /**
             * Determines if the {@code Graph} implementation supports threaded transactions which allow a transaction
             * to be executed across multiple threads via {@link com.tinkerpop.gremlin.structure.Transaction#create()}.
             */
            @FeatureDescriptor(name = FEATURE_THREADED_TRANSACTIONS)
            public default boolean supportsThreadedTransactions() {
                return true;
            }

            /**
             * Refers to the ability of the Graph implementation to fully isolate a change within a transaction across
             * threads.  There are a number of tests in the suite that start a transaction in one thread and do
             * assertions on the graph in a different thread to ensure that those changes are not reflected in the
             * other thread.  Not all graphs cleanly support that.
             * <br/>
             * This feature is a bit of a hazy one.  Implementers should evaluate the tests themselves to determine
             * if they can support the feature and if not, they should carefully document for users the capabilities
             * in this area.
             */
            @FeatureDescriptor(name = FEATURE_FULLY_ISOLATED_TRANSACTIONS)
            public default boolean supportsFullyIsolatedTransactions() {
                return true;
            }

            /**
             * Gets the features related to "graph memory" operation.
             */
            public default VariableFeatures memory() {
                return new VariableFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Vertex} operations.
         */
        public interface VertexFeatures extends FeatureSet {
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            public static final String FEATURE_ADD_VERTICES = "AddVertices";

            /**
             * Determines if a {@link Vertex} can have a user defined identifier.  Implementation that do not support
             * this feature will be expected to auto-generate unique identifiers.
             */
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can be added to the {@code Graph}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_VERTICES)
            public default boolean supportsAddVertices() {
                return true;
            }

            /**
             * Gets features related to "properties" on a {@link Vertex}.
             */
            public default VertexPropertyFeatures properties() {
                return new VertexPropertyFeatures() {
                };
            }
        }

        public interface EdgeFeatures extends FeatureSet {
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            public static final String FEATURE_ADD_EDGES = "AddEdges";

            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_ADD_EDGES)
            public default boolean supportsAddEdges() {
                return true;
            }

            public default EdgePropertyFeatures properties() {
                return new EdgePropertyFeatures() {
                };
            }
        }

        public interface VertexPropertyFeatures extends PropertyFeatures {
        }

        public interface EdgePropertyFeatures extends PropertyFeatures {
        }

        public interface PropertyFeatures extends DataTypeFeatures {
            public static final String FEATURE_PROPERTIES = "Properties";

            /**
             * If any of the features on PropertyFeatures is true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_PROPERTIES)
            public default boolean supportsProperties() {
                return supportsBooleanValues() || supportsByteValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsSerializableValues()
                        || supportsStringValues() || supportsUniformListValues() || supportsBooleanArrayValues()
                        || supportsByteArrayValues() || supportsDoubleArrayValues() || supportsFloatArrayValues()
                        || supportsIntegerArrayValues() || supportsLongArrayValues() || supportsStringArrayValues();
            }
        }

        public interface VariableFeatures extends DataTypeFeatures {
            public static final String FEATURE_VARIABLES = "Variables";

            /**
             * If any of the features on {@link com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures} is true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_VARIABLES)
            public default boolean supportsVariables() {
                return supportsBooleanValues() || supportsByteValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsSerializableValues()
                        || supportsStringValues() || supportsUniformListValues() || supportsBooleanArrayValues()
                        || supportsByteArrayValues() || supportsDoubleArrayValues() || supportsFloatArrayValues()
                        || supportsIntegerArrayValues() || supportsLongArrayValues() || supportsStringArrayValues();
            }
        }

        public interface DataTypeFeatures extends FeatureSet {
            public static final String FEATURE_BOOLEAN_VALUES = "BooleanValues";
            public static final String FEATURE_BYTE_VALUES = "ByteValues";
            public static final String FEATURE_DOUBLE_VALUES = "DoubleValues";
            public static final String FEATURE_FLOAT_VALUES = "FloatValues";
            public static final String FEATURE_INTEGER_VALUES = "IntegerValues";
            public static final String FEATURE_LONG_VALUES = "LongValues";
            public static final String FEATURE_MAP_VALUES = "MapValues";
            public static final String FEATURE_MIXED_LIST_VALUES = "MixedListValues";
            public static final String FEATURE_BOOLEAN_ARRAY_VALUES = "BooleanArrayValues";
            public static final String FEATURE_BYTE_ARRAY_VALUES = "ByteArrayValues";
            public static final String FEATURE_DOUBLE_ARRAY_VALUES = "DoubleArrayValues";
            public static final String FEATURE_FLOAT_ARRAY_VALUES = "FloatArrayValues";
            public static final String FEATURE_INTEGER_ARRAY_VALUES = "IntegerArrayValues";
            public static final String FEATURE_LONG_ARRAY_VALUES = "LongArrayValues";
            public static final String FEATURE_SERIALIZABLE_VALUES = "SerializableValues";
            public static final String FEATURE_STRING_ARRAY_VALUES = "StringArrayValues";
            public static final String FEATURE_STRING_VALUES = "StringValues";
            public static final String FEATURE_UNIFORM_LIST_VALUES = "UniformListValues";

            @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
            public default boolean supportsBooleanValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public default boolean supportsByteValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
            public default boolean supportsDoubleValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public default boolean supportsFloatValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public default boolean supportsIntegerValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_LONG_VALUES)
            public default boolean supportsLongValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public default boolean supportsMapValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public default boolean supportsMixedListValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public default boolean supportsBooleanArrayValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public default boolean supportsByteArrayValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public default boolean supportsDoubleArrayValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public default boolean supportsFloatArrayValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public default boolean supportsIntegerArrayValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public default boolean supportsStringArrayValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public default boolean supportsLongArrayValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public default boolean supportsSerializableValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_STRING_VALUES)
            public default boolean supportsStringValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public default boolean supportsUniformListValues() {
                return true;
            }
        }

        /**
         * A marker interface to identify any set of Features. There is no need to implement this interface.
         */
        public interface FeatureSet {
        }

        /**
         * Implementers should not override this method. Note that this method utilizes reflection to check for
         * feature support.
         */
        default boolean supports(final Class<? extends FeatureSet> featureClass, final String feature)
                throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            final Object instance;
            if (featureClass.equals(GraphFeatures.class))
                instance = this.graph();
            else if (featureClass.equals(VariableFeatures.class))
                instance = this.graph().memory();
            else if (featureClass.equals(VertexFeatures.class))
                instance = this.vertex();
            else if (featureClass.equals(VertexPropertyFeatures.class))
                instance = this.vertex().properties();
            else if (featureClass.equals(EdgeFeatures.class))
                instance = this.edge();
            else if (featureClass.equals(EdgePropertyFeatures.class))
                instance = this.edge().properties();
            else if (featureClass.equals(PropertyFeatures.class))
                throw new IllegalArgumentException(String.format(
                        "Do not reference PropertyFeatures directly in tests, utilize a specific instance: %s, %s",
                        EdgePropertyFeatures.class, VertexPropertyFeatures.class));
            else
                throw new IllegalArgumentException(String.format(
                        "Expecting featureClass to be a valid Feature instance and not %s", featureClass));

            return (Boolean) featureClass.getMethod("supports" + feature).invoke(instance);
        }
    }

    /**
     * Common exceptions to use with a graph.
     */
    public static class Exceptions {

        private static final boolean debug = Boolean.parseBoolean(System.getenv().getOrDefault("gremlin.structure.debug", "false"));

        public static UnsupportedOperationException variablesNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph variables");
        }

        public static UnsupportedOperationException transactionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support transactions");
        }

        public static UnsupportedOperationException graphComputerNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph computer");
        }

        public static UnsupportedOperationException vertexLookupsNotSupported() {
            return new UnsupportedOperationException("Graph does not support vertex lookups by id");
        }

        public static UnsupportedOperationException edgeLookupsNotSupported() {
            return new UnsupportedOperationException("Graph does not support edge lookups by id");
        }

        public static UnsupportedOperationException vertexAdditionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support adding vertices");
        }

        public static IllegalArgumentException vertexWithIdAlreadyExists(final Object id) {
            return new IllegalArgumentException(String.format("Vertex with id already exists: %s", id));
        }

        public static IllegalArgumentException edgeWithIdAlreadyExist(final Object id) {
            return new IllegalArgumentException(String.format("Edge with id already exists: %s", id));
        }

        public static IllegalArgumentException argumentCanNotBeNull(final String argument) {
            return new IllegalArgumentException(String.format("The provided argument can not be null: %s", argument));
        }

        public static NoSuchElementException elementNotFound(final Class<? extends Element> elementClass, final Object id) {
            return (null == id) ?
                    new NoSuchElementException("The " + elementClass.getSimpleName().toLowerCase() + " with id null does not exist in the graph") :
                    new NoSuchElementException("The " + elementClass.getSimpleName().toLowerCase() + " with id " + id + " of type " + id.getClass().getSimpleName() + " does not exist in the graph");
        }

        public static IllegalArgumentException onlyOneOrNoGraphComputerClass() {
            return new IllegalArgumentException("Provide either one or no graph computer class");
        }
    }
}
