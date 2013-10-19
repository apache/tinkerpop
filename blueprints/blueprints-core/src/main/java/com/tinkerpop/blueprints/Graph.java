package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;

import java.io.Closeable;

/**
 * An Graph is a container object for a collection of vertices and a collection edges.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Graph extends Closeable, Thing {

    public Vertex addVertex(Property... properties);

    public GraphQuery query();

    public GraphComputer compute();

    public void commit();

    public void rollback();

    public <T> Property<T, Graph> getProperty(String key);

    public <T> Property<T, Graph> setProperty(String key, T value);

    public <T> Property<T, Graph> removeProperty(String key);

    public default Graph.Features getFeatures() {
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
