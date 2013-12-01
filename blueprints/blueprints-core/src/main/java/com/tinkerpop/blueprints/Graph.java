package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;
import org.apache.commons.configuration.Configuration;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * An Graph is a container object for a collection of vertices, edges, and properties.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Graph extends AutoCloseable, Featureable {

    public static <G extends Graph> G open(Optional<Configuration> configuration) {
        throw new UnsupportedOperationException("Implementations must override this method");
    }

    public Vertex addVertex(Object... keyValues);

    public GraphQuery query();

    public GraphComputer compute();

    public Transaction tx();

    public <V> Graph.Property<V> getProperty(String key);

    public <V> Graph.Property<V> setProperty(String key, V value);

    public static Graph.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Property<V> extends com.tinkerpop.blueprints.Property<V> {

        public Graph getGraph();

        public static <V> Graph.Property<V> empty() {
            return new Graph.Property<V>() {
                @Override
                public String getKey() {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public V getValue() throws NoSuchElementException {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public boolean isPresent() {
                    return false;
                }

                @Override
                public void remove() {
                    throw Features.propertyDoesNotExist();
                }

                @Override
                public Graph getGraph() {
                    throw Features.propertyDoesNotExist();
                }
            };

        }
    }


    public interface Features extends com.tinkerpop.blueprints.Features {
        public default boolean supportsTransactions() {
            return true;
        }

        public default boolean supportsQuery() {
            return true;
        }

        public default boolean supportsComputer() {
            return true;
        }

        public static UnsupportedOperationException transactionsNotSupported() {
            return new UnsupportedOperationException("Graph does not support transactions");
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
