package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.javatuples.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An {@link Graph} is a container object for a collection of {@link Vertex}, {@link Edge}, and {@link Property}
 * objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Graph extends AutoCloseable {

    public class Key {

        private static final String HIDDEN_PREFIX = "%&%";

        public static final String hidden(final String key) {
            return HIDDEN_PREFIX.concat(key);
        }

        public static final String unHide(final String key) {
            return key.startsWith(HIDDEN_PREFIX) ? key.substring(HIDDEN_PREFIX.length()) : key;
        }

        public static final boolean isHidden(final String key) {
            return key.startsWith(HIDDEN_PREFIX);
        }
    }

    /**
     * Add a {@link Vertex} to a {@code Graph} given an optional series of key/value pairs.  These key/values
     * must be provided in an even number where the odd numbered arguments are {@link String} key values and the
     * even numbered arguments are the related property values.  Hidden properties can be set by specifying
     * the key as {@link com.tinkerpop.gremlin.structure.Graph.Key#hidden}.
     */
    public Vertex addVertex(final Object... keyValues);

    /**
     * Get a {@link Vertex} given its unique identifier.
     *
     * @throws NoSuchElementException if the element is not found.
     */
    public default Vertex v(final Object id) {
        if (null == id) throw Graph.Exceptions.elementNotFound();
        return (Vertex) this.V().has(Element.ID, id).next();
    }

    /**
     * Get a {@link Edge} given its unique identifier.
     *
     * @throws NoSuchElementException if the element is not found.
     */
    public default Edge e(final Object id) {
        if (null == id) throw Graph.Exceptions.elementNotFound();
        return (Edge) this.E().has(Element.ID, id).next();
    }

    /**
     * Starts a {@link GraphTraversal} over all vertices.
     */
    public GraphTraversal<Vertex, Vertex> V();

    /**
     * Starts a {@link GraphTraversal} over all edges.
     */
    public GraphTraversal<Edge, Edge> E();

    /**
     * Constructs a new {@link Traversal} over the {@code Graph}
     *
     * @param traversalClass a {@link Traversal} implementation to use
     */
    public default <T extends Traversal> T traversal(final Class<T> traversalClass) {
        try {
            final T t = (T) traversalClass.getMethod(Traversal.OF).invoke(null);
            t.memory().set("g", this);
            return t;
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public default GraphTraversal of() {
        return GraphTraversal.of();
    }

    public <C extends GraphComputer> C compute(final Class<C>... graphComputerClass);

    /**
     * Configure and control the transactions for those graphs that support this feature.
     */
    public Transaction tx();

    public <V extends Variables> V variables();

    public interface Variables {

        public Set<String> keys();

        public <R> R get(final String key);

        public void set(final String key, Object value);

        public <R> R remove(final String key);

        public default Map<String, Object> asMap() {
            final Map<String, Object> map = keys().stream()
                    .map(key -> Pair.<String, Object>with(key, get(key)))
                    .collect(Collectors.toMap(kv -> kv.getValue0(), Pair::getValue1));
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

    public default Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features {
        public default GraphFeatures graph() {
            return new GraphFeatures() {
            };
        }

        public default VertexFeatures vertex() {
            return new VertexFeatures() {
            };
        }

        public default EdgeFeatures edge() {
            return new EdgeFeatures() {
            };
        }

        public interface GraphFeatures extends FeatureSet {
            public static final String FEATURE_COMPUTER = "Computer";
            public static final String FEATURE_TRANSACTIONS = "Transactions";
            public static final String FEATURE_PERSISTENCE = "Persistence";
            public static final String FEATURE_THREADED_TRANSACTIONS = "ThreadedTransactions";
            public static final String FEATURE_FULLY_ISOLATED_TRANSACTIONS = "FullyIsolatedTransactions";

            @FeatureDescriptor(name = FEATURE_COMPUTER)
            public default boolean supportsComputer() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_PERSISTENCE)
            public default boolean supportsPersistence() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_TRANSACTIONS)
            public default boolean supportsTransactions() {
                return true;
            }

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

            public default VariableFeatures memory() {
                return new VariableFeatures() {
                };
            }
        }

        public interface VertexFeatures extends FeatureSet {
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";
            public static final String FEATURE_ADD_VERTICES = "AddVertices";

            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_ADD_VERTICES)
            public default boolean supportsAddVertices() {
                return true;
            }

            public default VertexPropertyFeatures properties() {
                return new VertexPropertyFeatures() {
                };
            }

            public default VertexAnnotationFeatures annotations() {
                return new VertexAnnotationFeatures() {
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

        // todo: how do we sort out features in relation to property types, arrays, ids, memory, etc. (e.g. graph supports id assignment, but only for Longs)

        public interface PropertyFeatures extends DataTypeFeatures {
            public static final String FEATURE_PROPERTIES = "Properties";

            /**
             * If any of the features on PropertyFeatures is true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_PROPERTIES)
            public default boolean supportsProperties() {
                return supportsBooleanValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsPrimitiveArrayValues()
                        || supportsPrimitiveArrayValues() || supportsSerializableValues() || supportsStringValues()
                        || supportsUniformListValues();
            }
        }

        public interface VariableFeatures extends DataTypeFeatures {
            public static final String FEATURE_VARIABLES = "Variables";

            /**
             * If any of the features on {@link com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures} is true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_VARIABLES)
            public default boolean supportsVariables() {
                return supportsBooleanValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsPrimitiveArrayValues()
                        || supportsPrimitiveArrayValues() || supportsSerializableValues() || supportsStringValues()
                        || supportsUniformListValues();
            }
        }

        public interface VertexAnnotationFeatures extends AnnotationFeatures {
        }

        public interface AnnotationFeatures extends DataTypeFeatures {
            public static final String FEATURE_ANNOTATIONS = "Annotations";

            /**
             * If any of the features on {@link AnnotationFeatures} is true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_ANNOTATIONS)
            public default boolean supportsAnnotations() {
                return supportsBooleanValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMixedListValues() || supportsPrimitiveArrayValues()
                        || supportsPrimitiveArrayValues() || supportsSerializableValues() || supportsStringValues()
                        || supportsUniformListValues();
            }
        }


        public interface DataTypeFeatures extends FeatureSet {
            public static final String FEATURE_BOOLEAN_VALUES = "BooleanValues";
            public static final String FEATURE_DOUBLE_VALUES = "DoubleValues";
            public static final String FEATURE_FLOAT_VALUES = "FloatValues";
            public static final String FEATURE_INTEGER_VALUES = "IntegerValues";
            public static final String FEATURE_LONG_VALUES = "LongValues";
            public static final String FEATURE_MAP_VALUES = "MapValues";
            public static final String FEATURE_MIXED_LIST_VALUES = "MixedListValues";
            public static final String FEATURE_PRIMITIVE_ARRAY_VALUES = "PrimitiveArrayValues";
            public static final String FEATURE_SERIALIZABLE_VALUES = "SerializableValues";
            public static final String FEATURE_STRING_VALUES = "StringValues";
            public static final String FEATURE_UNIFORM_LIST_VALUES = "UniformListValues";

            @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
            public default boolean supportsBooleanValues() {
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

            @FeatureDescriptor(name = FEATURE_PRIMITIVE_ARRAY_VALUES)
            public default boolean supportsPrimitiveArrayValues() {
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
            else if (featureClass.equals(VertexAnnotationFeatures.class))
                instance = this.vertex().annotations();
            else if (featureClass.equals(EdgeFeatures.class))
                instance = this.edge();
            else if (featureClass.equals(EdgePropertyFeatures.class))
                instance = this.edge().properties();
            else if (featureClass.equals(PropertyFeatures.class))
                throw new IllegalArgumentException(String.format(
                        "Do not reference PropertyFeatures directly in tests, utilize a specific instance: %s, %s",
                        EdgePropertyFeatures.class, VertexPropertyFeatures.class));
            else if (featureClass.equals(AnnotationFeatures.class))
                throw new IllegalArgumentException(String.format(
                        "Do not reference AnnotationFeatures directly in tests, utilize a specific instance: %s, %s",
                        VertexAnnotationFeatures.class, VariableFeatures.class));
            else
                throw new IllegalArgumentException(String.format(
                        "Expecting featureClass to be a valid Feature instance and not %s", featureClass));

            return (Boolean) featureClass.getMethod("supports" + feature).invoke(instance);
        }
    }

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

        public static NoSuchElementException elementNotFound() {
            // if in debug mode then write a regular exception so the whole stack trace comes with it.
            // very hard to figure out problems in the stack without that.
            if (debug)
                return new NoSuchElementException();
            else
                return FastNoSuchElementException.instance();
        }

        public static IllegalArgumentException onlyOneOrNoGraphComputerClass() {
            return new IllegalArgumentException("Provide either one or no graph computer class");
        }
    }
}
