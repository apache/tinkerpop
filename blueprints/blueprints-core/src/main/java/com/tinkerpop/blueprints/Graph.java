package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.GraphQuery;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.Closeable;
import java.io.File;
import java.util.Map;

/**
 * An Graph is a container object for a collection of vertices and a collection edges.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
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

    /**
     * Open a graph.  See each Graph instance for its configuration options.
     *
     * @param configuration A configuration object that specifies the minimally required properties for a Graph instance.
     *                      This minimum is determined by the Graph instance itself.
     * @return A Graph instance.
     */
    public static Graph open(final Configuration configuration) {
        final String clazz = configuration.getString("blueprints.graph", null);

        if (clazz == null) {
            throw new RuntimeException("Configuration must contain a valid 'blueprints.graph' setting");
        }

        Class graphClass;
        try {
            graphClass = Class.forName(clazz);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(String.format("GraphFactory could not find [%s].  Ensure that the jar is in the classpath.", clazz));
        }

        Graph g;
        try {
            // directly instantiate if it is a Graph class otherwise try to use a factory
            if (Graph.class.isAssignableFrom(graphClass)) {
                g = (Graph) graphClass.getConstructor(Configuration.class).newInstance(configuration);
            } else {
                g = (Graph) graphClass.getMethod("open", Configuration.class).invoke(null, configuration);
            }
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException(String.format("GraphFactory can only instantiate Graph implementations that have a constructor with a single Apache Commons Configuration argument. [%s] does not seem to have one.", clazz));
        } catch (Exception ex) {
            throw new RuntimeException(String.format("GraphFactory could not instantiate this Graph implementation [%s].", clazz), ex);
        }

        return g;
    }

    /**
     * Open a graph.  See each Graph instance for its configuration options.
     *
     * @param configurationFile The location of a configuration file that specifies the minimally required properties
     *                          for a Graph instance. This minimum is determined by the Graph instance itself.
     * @return A Graph instance.
     */
    public static Graph open(final String configurationFile) {
        final File dirOrFile = new File(configurationFile);
        if (!dirOrFile.isFile())
            throw new IllegalArgumentException("Location of configuration must be a file");

        try {
            return open(new PropertiesConfiguration(dirOrFile));
        } catch (ConfigurationException e) {
            throw new IllegalArgumentException("Could not load configuration at: " + dirOrFile, e);
        }
    }

    /**
     * Open a graph. See each Graph instance for its configuration options.
     *
     * @param configuration A Map based configuration that will be converted to an Apache configuration object
     *                      via MapConfiguration and passed to the appropriate overload.
     * @return A Graph instance.
     */
    public static Graph open(final Map configuration) {
        return open(new MapConfiguration(configuration));
    }
}
