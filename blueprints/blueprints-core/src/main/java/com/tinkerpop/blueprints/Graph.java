package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;
import org.apache.commons.configuration.Configuration;

import java.util.Optional;

/**
 * An Graph is a container object for a collection of vertices and a collection edges.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Graph extends AutoCloseable, Thing {

    public static <G extends Graph> G open(Optional<Configuration> configuration) {
        throw new UnsupportedOperationException("Implementations must override this method");
    }

    public Vertex addVertex(Property... properties);

    public GraphQuery query();

    public GraphComputer compute();

    public void commit();

    public void rollback();

    public <V> Property<V, Graph> getProperty(String key);

    public <V> Property<V, Graph> setProperty(String key, V value);

    public <V> Property<V, Graph> removeProperty(String key);

    public static Graph.Features getFeatures() {
        return new Features() {
        };
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
    }
}
