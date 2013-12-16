package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;
import com.tinkerpop.blueprints.util.FeatureDescriptor;
import org.apache.commons.configuration.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * An Graph is a container object for a collection of vertices, edges, and properties.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Graph extends AutoCloseable {

    public static <G extends Graph> G open(Optional<Configuration> configuration) {
        throw new UnsupportedOperationException("Implementations must override this method");
    }

    public Vertex addVertex(Object... keyValues);

    public GraphQuery query();

    public GraphComputer compute();

    public Transaction tx();

    public <V> Graph.Property<V> getProperty(String key);

    public <V> Graph.Property<V> setProperty(String key, V value);

    public default Graph.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Property<V> extends com.tinkerpop.blueprints.Property<V> {

        public Graph getGraph();

        public static <V> Graph.Property<V> empty() {
            return new Graph.Property<V>() {
                @Override
                public String getKey() {
                    throw Property.Exceptions.propertyDoesNotExist();
                }

                @Override
                public V getValue() throws NoSuchElementException {
                    throw Property.Exceptions.propertyDoesNotExist();
                }

                @Override
                public boolean isPresent() {
                    return false;
                }

                @Override
                public void remove() {
                    throw Property.Exceptions.propertyDoesNotExist();
                }

                @Override
                public Graph getGraph() {
                    throw Property.Exceptions.propertyDoesNotExist();
                }
            };

        }
    }

    public interface Features {
        public default GraphFeatures graph() {
            return new GraphFeatures() { };
        }

        public default VertexFeatures vertex() {
            return new VertexFeatures() { };
        }

        public default PropertyFeatures property() {
            return new PropertyFeatures() { };
        }

        public interface GraphFeatures extends FeatureSet {
            public static final String FEATURE_TRANSACTIONS = "Transactions";
            public static final String FEATURE_QUERY = "Query";
            public static final String FEATURE_COMPUTER = "Computer";

            @FeatureDescriptor(name = FEATURE_TRANSACTIONS)
            public default boolean supportsTransactions() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_QUERY)
            public default boolean supportsQuery() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_COMPUTER)
            public default boolean supportsComputer() {
                return true;
            }
        }

        public interface VertexFeatures extends FeatureSet {
            public static final String FEATURE_USER_SUPPLIED_IDS = "UserSuppliedIds";

            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public default boolean supportsUserSuppliedIds() {
                return true;
            }
        }

        public interface PropertyFeatures extends FeatureSet {
            public static final String FEATURE_META_PROPERTIES = "MetaProperties";
            public static final String FEATURE_STRING_VALUES = "StringValues";
            public static final String FEATURE_INTEGER_VALUES = "IntegerValues";

            @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
            public default boolean supportsMetaProperties() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_STRING_VALUES)
            public default boolean supportsStringValues() {
                return true;
            }

            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public default boolean supportsIntegerValues() {
                return true;
            }
        }

        /**
         * A marker interface to identify any set of Features. There is no need to implement this interface.
         */
        public interface FeatureSet{}

        /**
         * Implementers should not override this method. Note that this method utilizes reflection to check for
         * feature support.
         */
        default boolean supports(final Class<? extends FeatureSet> featureClass, final String feature)
                throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
            final Object instance;
            if (featureClass.equals(GraphFeatures.class))
                instance = this.graph();
            else if (featureClass.equals(PropertyFeatures.class))
                instance = this.property();
            else if (featureClass.equals(VertexFeatures.class))
                instance = this.vertex();
            else
                throw new IllegalArgumentException(String.format(
                        "Expecting featureClass to be a valid Feature instance and not %s", featureClass));

            return (Boolean) featureClass.getMethod("supports" + feature).invoke(instance);
        }
    }

    public static class Exceptions {
        public static UnsupportedOperationException transactionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support transactions");
        }

        public static UnsupportedOperationException graphQueryNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph query");
        }

        public static UnsupportedOperationException graphComputerNotSupported() {
            return new UnsupportedOperationException("Graph does not support graph computer");
        }

        public static IllegalArgumentException vertexIdCanNotBeNull() {
            return new IllegalArgumentException("Vertex id can not be null");
        }

        public static IllegalArgumentException edgeIdCanNotBeNull() {
            return new IllegalArgumentException("Edge id can not be null");
        }

        public static IllegalArgumentException vertexWithIdAlreadyExists(final Object id) {
            return new IllegalArgumentException("Vertex with id already exists: " + id);
        }

        public static IllegalArgumentException edgeWithIdAlreadyExist(final Object id) {
            return new IllegalArgumentException("Edge with id already exists: " + id);
        }

        public static IllegalStateException vertexWithIdDoesNotExist(final Object id) {
            return new IllegalStateException("Vertex with id does not exist: " + id);
        }
    }
}
