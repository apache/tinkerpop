package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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

    public static final String HIDDEN_PREFIX = "%&%";

    /**
     * Creates a new {@link Graph} instance.  All graphs require that this method be overridden.  It is enforced by the
     * Blueprints test suite.
     */
    public static <G extends Graph> G open(final Optional<Configuration> configuration) {
        throw new UnsupportedOperationException("Implementations must override this method");
    }

    public Vertex addVertex(final Object... keyValues);

    public default Vertex v(final Object id) {
        return (Vertex) this.V().has(Element.ID, id).next();
    }

    public default Edge e(final Object id) {
        return (Edge) this.E().has(Element.ID, id).next();
    }

    public GraphTraversal<Vertex, Vertex> V();

    public GraphTraversal<Edge, Edge> E();

    public default <T extends Traversal> T traversal(final Class<T> traversalClass) {
        try {
            return (T) traversalClass.getMethod(Traversal.OF).invoke(null);
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public GraphComputer compute();

    public Transaction tx();

    public <M extends Memory> M memory();

    public interface Memory {

        public class Key {

            private Key() {

            }

            public static String hidden(final String key) {
                return HIDDEN_PREFIX.concat(key);
            }
        }

        public Set<String> getVariables();

        public <R> R get(final String variable);

        public void set(final String variable, Object value);

        public void setIfAbsent(final String variable, final Object value);

        public long incr(final String variable, final long delta);

        public long decr(final String variable, final long delta);

        public boolean and(final String variable, final boolean bool);

        public boolean or(final String variable, final boolean bool);

        public Graph getGraph();

        public default Map<String, Object> asMap() {
            final Map<String, Object> map = getVariables().stream()
                    .map(key -> Pair.<String, Object>with(key, get(key)))
                    .collect(Collectors.toMap(kv -> kv.getValue0(), Pair::getValue1));
            return Collections.unmodifiableMap(map);
        }

        public interface Computer extends Memory {

            public int getIteration();

            public long getRuntime();

            public default boolean isInitialIteration() {
                return this.getIteration() == 0;
            }

            public interface System extends Computer {

                public void incrIteration();

                public void setRuntime(final long runtime);
            }
        }

        public static class Exceptions {

            public static IllegalArgumentException memoryKeyCanNotBeEmpty() {
                return new IllegalArgumentException("Graph annotation key can not be the empty string");
            }

            public static IllegalArgumentException memoryKeyCanNotBeNull() {
                return new IllegalArgumentException("Graph annotation key can not be null");
            }

            public static IllegalArgumentException memoryValueCanNotBeNull() {
                return new IllegalArgumentException("Graph annotation value can not be null");
            }

            public static UnsupportedOperationException dataTypeOfMemoryValueNotSupported(final Object val) {
                return new UnsupportedOperationException(String.format("Graph annotation value [%s] is of type %s is not supported", val, val.getClass()));
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
            // todo: do we need this memory feature and the Graph.Memory feature???
            public static final String FEATURE_MEMORY = "Memory";
            public static final String FEATURE_COMPUTER = "Computer";
            public static final String FEATURE_TRANSACTIONS = "Transactions";
            public static final String FEATURE_PERSISTENCE = "Persistence";

            @FeatureDescriptor(name = FEATURE_MEMORY)
            public default boolean supportsMemory() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_COMPUTER)
            public default boolean supportsComputer() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_PERSISTENCE)
            public default boolean supportsPersistence() {
                return true;
            }

            // todo: threaded tx feature?
            @FeatureDescriptor(name = FEATURE_TRANSACTIONS)
            public default boolean supportsTransactions() {
                return true;
            }

            public default MemoryFeatures memory() {
                return new MemoryFeatures() {
                };
            }
        }

        public interface VertexFeatures extends FeatureSet {
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";

            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
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

            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
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
                return supportsBooleanValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMetaProperties() || supportsMixedListValues() || supportsPrimitiveArrayValues()
                        || supportsPrimitiveArrayValues() || supportsSerializableValues() || supportsStringValues()
                        || supportsUniformListValues();
            }
        }

        public interface MemoryFeatures extends DataTypeFeatures {
            public static final String FEATURE_MEMORY = "Memory";

            /**
             * If any of the features on {@link MemoryFeatures} is true then this value must be true.
             */
            @FeatureDescriptor(name = FEATURE_MEMORY)
            public default boolean supportsMemory() {
                return supportsBooleanValues() || supportsDoubleValues() || supportsFloatValues()
                        || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                        || supportsMetaProperties() || supportsMixedListValues() || supportsPrimitiveArrayValues()
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
                        || supportsMetaProperties() || supportsMixedListValues() || supportsPrimitiveArrayValues()
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
            public static final String FEATURE_META_PROPERTIES = "MetaProperties";  // todo: remove meta properties feature
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

            @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
            public default boolean supportsMetaProperties() {
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
            else if (featureClass.equals(MemoryFeatures.class))
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
                        VertexAnnotationFeatures.class, MemoryFeatures.class));
            else
                throw new IllegalArgumentException(String.format(
                        "Expecting featureClass to be a valid Feature instance and not %s", featureClass));

            return (Boolean) featureClass.getMethod("supports" + feature).invoke(instance);
        }
    }

    public static class Exceptions {

        public static UnsupportedOperationException memoryNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph memory");
        }

        public static UnsupportedOperationException transactionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support transactions");
        }

        public static UnsupportedOperationException graphComputerNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph computer");
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
    }
}
