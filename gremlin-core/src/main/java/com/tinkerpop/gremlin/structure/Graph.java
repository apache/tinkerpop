package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.javatuples.Pair;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link Graph} is a container object for a collection of {@link Vertex}, {@link Edge}, {@link VertexProperty},
 * and {@link Property} objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Graph extends AutoCloseable {

    public static final String GRAPH = "gremlin.graph";

    /**
     * This should only be used by vendors to create keys in a namespace safe from users.
     */
    public class System {

        /**
         * The prefix to denote that a key is a system key.
         */
        private static final String SYSTEM_PREFIX = "^";
        private static final int SYSTEM_PREFIX_LENGTH = SYSTEM_PREFIX.length();

        /**
         * Turn the provided key into a system key. If the key is already a system key, return key.
         *
         * @param key The key to make a system key
         * @return The system key
         */
        public static String system(final String key) {
            return isSystem(key) ? key : SYSTEM_PREFIX.concat(key);
        }

        /**
         * Turn the provided system key into an non-system key. If the key is not a system key, return key.
         *
         * @param key The system key
         * @return The non-system representation of the key
         */
        public static String unSystem(final String key) {
            return isSystem(key) ? key.substring(SYSTEM_PREFIX_LENGTH) : key;
        }

        /**
         * Determines whether the provided key is a system key or not.
         *
         * @param key The key to check for system status
         * @return Whether the provided key is a system key or not
         */
        public static boolean isSystem(final String key) {
            return key.startsWith(SYSTEM_PREFIX);
        }
    }

    /**
     * Key is a helper class for manipulating keys wherever they may be (e.g. properties, sideEffects, sideEffects, etc.)
     */
    public class Key {

        /**
         * The prefix to denote that a key is hidden.
         */
        private static final String HIDDEN_PREFIX = "~";
        private static final int HIDDEN_PREFIX_LENGTH = HIDDEN_PREFIX.length();

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
            return isHidden(key) ? key.substring(HIDDEN_PREFIX_LENGTH) : key;
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
     * Add a {@link Vertex} to the graph with provided vertex label.
     *
     * @param label the label of the vertex
     * @return The newly created labeled vertex
     */
    public default Vertex addVertex(final String label) {
        return this.addVertex(T.label, label);
    }

    /**
     * Get a {@link Vertex} given its unique identifier.
     *
     * @param id The unique identifier of the vertex to locate
     * @throws NoSuchElementException if the vertex is not found.
     */
    public default Vertex v(final Object id) throws NoSuchElementException {
        if (null == id) throw Graph.Exceptions.elementNotFound(Vertex.class, null);
        return (Vertex) this.V().has(T.id, id).next();
    }

    /**
     * Get a {@link Edge} given its unique identifier.
     *
     * @param id The unique identifier of the edge to locate
     * @throws NoSuchElementException if the edge is not found.
     */
    public default Edge e(final Object id) throws NoSuchElementException {
        if (null == id) throw Graph.Exceptions.elementNotFound(Edge.class, null);
        return (Edge) this.E().has(T.id, id).next();
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
     * @return A graph computer for processing this graph
     */
    public GraphComputer compute(final Class... graphComputerClass);

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
    public default Features features() {
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
             * TinkerGraph does not support this feature as it is a pure in-sideEffects graph.
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
             * Gets the features related to "graph sideEffects" operation.
             */
            public default VariableFeatures variables() {
                return new VariableFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Vertex} operations.
         */
        public interface VertexFeatures extends ElementFeatures {
            public static final String FEATURE_ADD_VERTICES = "AddVertices";
            public static final String FEATURE_MULTI_PROPERTIES = "MultiProperties";
            public static final String FEATURE_META_PROPERTIES = "MetaProperties";
            public static final String FEATURE_REMOVE_VERTICES = "RemoveVertices";

            /**
             * Determines if a {@link Vertex} can be added to the {@code Graph}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_VERTICES)
            public default boolean supportsAddVertices() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can be removed from the {@code Graph}.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_VERTICES)
            public default boolean supportsRemoveVertices() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can support multiple properties with the same key.
             */
            @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
            public default boolean supportsMultiProperties() {
                return true;
            }

            /**
             * Determines if a {@link Vertex} can support properties on vertex properties.  It is assumed that a
             * graph will support all the same data types for meta-properties that are supported for regular
             * properties.
             */
            @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
            public default boolean supportsMetaProperties() {
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

        /**
         * Features that are related to {@link Edge} operations.
         */
        public interface EdgeFeatures extends ElementFeatures {
            public static final String FEATURE_ADD_EDGES = "AddEdges";
            public static final String FEATURE_REMOVE_EDGES = "RemoveEdges";

            /**
             * Determines if an {@link Edge} can be added to a {@code Vertex}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_EDGES)
            public default boolean supportsAddEdges() {
                return true;
            }

            /**
             * Determines if an {@link Edge} can be removed from a {@code Vertex}.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_EDGES)
            public default boolean supportsRemoveEdges() {
                return true;
            }

            /**
             * Gets features related to "properties" on an {@link Edge}.
             */
            public default EdgePropertyFeatures properties() {
                return new EdgePropertyFeatures() {
                };
            }
        }

        /**
         * Features that are related to {@link Element} objects.  This is a base interface.
         */
        public interface ElementFeatures extends FeatureSet {
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            public static final String FEATURE_NUMERIC_IDS = "NumericIds";
            public static final String FEATURE_STRING_IDS = "StringIds";
            public static final String FEATURE_UUID_IDS = "UuidIds";
            public static final String FEATURE_CUSTOM_IDS = "CustomIds";
            public static final String FEATURE_ANY_IDS = "AnyIds";
            public static final String FEATURE_ADD_PROPERTY = "AddProperty";
            public static final String FEATURE_REMOVE_PROPERTY = "RemoveProperty";

            /**
             * Determines if an {@link Element} allows properties to be added.  This feature is set independently from
             * supporting "data types" and refers to support of calls to {@link Element#property(String, Object)}.
             */
            @FeatureDescriptor(name = FEATURE_ADD_PROPERTY)
            public default boolean supportsAddProperty() {
                return true;
            }

            /**
             * Determines if an {@link Element} allows properties to be removed.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
            public default boolean supportsRemoveProperty() {
                return true;
            }

            /**
             * Determines if an {@link Element} can have a user defined identifier.  Implementation that do not support
             * this feature will be expected to auto-generate unique identifiers.
             */
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has numeric identifiers.
             */
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            public default boolean supportsNumericIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has string identifiers.
             */
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public default boolean supportsStringIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has UUID identifiers.
             */
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public default boolean supportsUuidIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} has custom identifiers where "custom" refers to an implementation
             * defined object.
             */
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public default boolean supportsCustomIds() {
                return true;
            }

            /**
             * Determines if an {@link Element} any Java object is a suitable identifier.
             */
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public default boolean supportsAnyIds() {
                return true;
            }
        }

        /**
         * Features that are related to {@link Vertex} {@link Property} objects.
         */
        public interface VertexPropertyFeatures extends PropertyFeatures {
            public static final String FEATURE_ADD_PROPERTY = "AddProperty";
            public static final String FEATURE_REMOVE_PROPERTY = "RemoveProperty";
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            public static final String FEATURE_NUMERIC_IDS = "NumericIds";
            public static final String FEATURE_STRING_IDS = "StringIds";
            public static final String FEATURE_UUID_IDS = "UuidIds";
            public static final String FEATURE_CUSTOM_IDS = "CustomIds";
            public static final String FEATURE_ANY_IDS = "AnyIds";

            /**
             * Determines if a {@link VertexProperty} allows properties to be added.
             */
            @FeatureDescriptor(name = FEATURE_ADD_PROPERTY)
            public default boolean supportsAddProperty() {
                return true;
            }

            /**
             * Determines if a {@link VertexProperty} allows properties to be removed.
             */
            @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
            public default boolean supportsRemoveProperty() {
                return true;
            }

            /**
             * Determines if a {@link VertexProperty} allows an identifier to be assigned to it.
             */
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has numeric identifiers.
             */
            @FeatureDescriptor(name = FEATURE_NUMERIC_IDS)
            public default boolean supportsNumericIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has string identifiers.
             */
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public default boolean supportsStringIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has UUID identifiers.
             */
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public default boolean supportsUuidIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} has custom identifiers where "custom" refers to an implementation
             * defined object.
             */
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public default boolean supportsCustomIds() {
                return true;
            }

            /**
             * Determines if an {@link VertexProperty} any Java object is a suitable identifier.
             */
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public default boolean supportsAnyIds() {
                return true;
            }
        }

        /**
         * Features that are related to {@link Edge} {@link Property} objects.
         */
        public interface EdgePropertyFeatures extends PropertyFeatures {
        }

        /**
         * A base interface for {@link Edge} or {@link Vertex} {@link Property} features.
         */
        public interface PropertyFeatures extends DataTypeFeatures {
            public static final String FEATURE_PROPERTIES = "Properties";

            /**
             * Determines if an {@link Element} allows for the processing of at least one data type defined by the
             * features.  In this case "processing" refers to at least "reading" the data type. If any of the
             * features on {@link PropertyFeatures} is true then this value must be true.
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

        /**
         * Features for {@link com.tinkerpop.gremlin.structure.Graph.Variables}.
         */
        public interface VariableFeatures extends DataTypeFeatures {
            public static final String FEATURE_VARIABLES = "Variables";

            /**
             * If any of the features on {@link com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures} is
             * true then this value must be true.
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

        /**
         * Base interface for features that relate to supporting different data types.
         */
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

            /**
             * Supports setting of a boolean value.
             */
            @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
            public default boolean supportsBooleanValues() {
                return true;
            }

            /**
             * Supports setting of a byte value.
             */
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public default boolean supportsByteValues() {
                return true;
            }

            /**
             * Supports setting of a double value.
             */
            @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
            public default boolean supportsDoubleValues() {
                return true;
            }

            /**
             * Supports setting of a float value.
             */
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public default boolean supportsFloatValues() {
                return true;
            }

            /**
             * Supports setting of a integer value.
             */
            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public default boolean supportsIntegerValues() {
                return true;
            }

            /**
             * Supports setting of a long value.
             */
            @FeatureDescriptor(name = FEATURE_LONG_VALUES)
            public default boolean supportsLongValues() {
                return true;
            }

            /**
             * Supports setting of a {@code Map} value.  The assumption is that the {@code Map} can contain
             * arbitrary serializable values that may or may not be defined as a feature itself.
             */
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public default boolean supportsMapValues() {
                return true;
            }

            /**
             * Supports setting of a {@code List} value.  The assumption is that the {@code List} can contain
             * arbitrary serializable values that may or may not be defined as a feature itself.  As this
             * {@code List} is "mixed" it does not need to contain objects of the same type.
             *
             * @see #supportsMixedListValues()
             */
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public default boolean supportsMixedListValues() {
                return true;
            }

            /**
             * Supports setting of an array of boolean values.
             */
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public default boolean supportsBooleanArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of byte values.
             */
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public default boolean supportsByteArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of double values.
             */
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public default boolean supportsDoubleArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of float values.
             */
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public default boolean supportsFloatArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of integer values.
             */
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public default boolean supportsIntegerArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of string values.
             */
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public default boolean supportsStringArrayValues() {
                return true;
            }

            /**
             * Supports setting of an array of long values.
             */
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public default boolean supportsLongArrayValues() {
                return true;
            }

            /**
             * Supports setting of a Java serializable value.
             */
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public default boolean supportsSerializableValues() {
                return true;
            }

            /**
             * Supports setting of a long value.
             */
            @FeatureDescriptor(name = FEATURE_STRING_VALUES)
            public default boolean supportsStringValues() {
                return true;
            }

            /**
             * Supports setting of a {@code List} value.  The assumption is that the {@code List} can contain
             * arbitrary serializable values that may or may not be defined as a feature itself.  As this
             * {@code List} is "uniform" it must contain objects of the same type.
             *
             * @see #supportsMixedListValues()
             */
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
                instance = this.graph().variables();
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

        private static final boolean debug = Boolean.parseBoolean(java.lang.System.getenv().getOrDefault("gremlin.structure.debug", "false"));

        public static UnsupportedOperationException variablesNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph variables");
        }

        public static UnsupportedOperationException transactionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support transactions");
        }

        public static UnsupportedOperationException graphComputerNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph computer");
        }

        public static IllegalArgumentException graphDoesNotSupportProvidedGraphComputer(final Class graphComputerClass) {
            return new IllegalArgumentException("Graph does not support the provided graph computer: " + graphComputerClass.getSimpleName());
        }

        public static UnsupportedOperationException vertexAdditionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support adding vertices");
        }

        public static IllegalArgumentException vertexWithIdAlreadyExists(final Object id) {
            return new IllegalArgumentException(String.format("Vertex with id already exists: %s", id));
        }

        public static IllegalArgumentException edgeWithIdAlreadyExists(final Object id) {
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

    /**
     * Defines the test suite that the implementer has decided to support and represents publicly as "passing".
     * Marking the {@link Graph} instance with this class allows that particular test suite to run.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Repeatable(OptIns.class)
    @Inherited
    public @interface OptIn {
        public static String SUITE_STRUCTURE_STANDARD = "com.tinkerpop.gremlin.structure.StructureStandardSuite";
        public static String SUITE_STRUCTURE_PERFORMANCE = "com.tinkerpop.gremlin.structure.StructurePerformanceSuite";
        public static String SUITE_PROCESS_COMPUTER = "com.tinkerpop.gremlin.process.ProcessComputerSuite";
        public static String SUITE_PROCESS_STANDARD = "com.tinkerpop.gremlin.process.ProcessStandardSuite";
        public static String SUITE_GROOVY_PROCESS_STANDARD = "com.tinkerpop.gremlin.process.GroovyProcessStandardSuite";
        public static String SUITE_GROOVY_PROCESS_COMPUTER = "com.tinkerpop.gremlin.process.GroovyProcessComputerSuite";
        public static String SUITE_GROOVY_ENVIRONMENT = "com.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite";
        public static String SUITE_GROOVY_ENVIRONMENT_INTEGRATE = "com.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite";

        /**
         * The test suite class to opt in to.
         */
        public String value();
    }

    /**
     * Holds a collection of {@link OptIn} enabling multiple {@link OptIn} to be applied to a
     * single suite.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface OptIns {
        OptIn[] value();
    }

    /**
     * Defines a test in the suite that the implementer does not want to run.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Repeatable(OptOuts.class)
    @Inherited
    public @interface OptOut {
        /**
         * The test class to opt out of.
         */
        public String test();

        /**
         * The specific name of the test method to opt out of.
         */
        public String method();

        /**
         * The reason the implementation is opting out of this test.
         */
        public String reason();
    }

    /**
     * Holds a collection of {@link OptOut} enabling multiple {@link OptOut} to be applied to a
     * single suite.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface OptOuts {
        OptOut[] value();
    }
}
