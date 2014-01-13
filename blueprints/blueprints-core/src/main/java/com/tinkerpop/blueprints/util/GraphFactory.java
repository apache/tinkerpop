package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.strategy.GraphStrategy;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
import java.util.Map;
import java.util.Optional;

/**
 * Factory to construct new {@link Graph} instances from a {@link Configuration} object or properties file.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphFactory {

    /**
     * Open a graph.  See each {@link Graph} instance for its configuration options.
     *
     * @param configuration A configuration object that specifies the minimally required properties for a {@link Graph}
     *                      instance. This minimum is determined by the {@link Graph} instance itself.
     * @param strategy A {@link com.tinkerpop.blueprints.strategy.GraphStrategy} to plug into the underlying {@link Graph} being constructed.
     * @return A {@link Graph} instance.
     */
    public static Graph open(final Configuration configuration, final Optional<? extends GraphStrategy> strategy) {
        if (null == configuration)
            throw new IllegalArgumentException("Configuration argument cannot be null");

        final String clazz = configuration.getString("blueprints.graph", null);
        if (null == clazz)
            throw new RuntimeException("Configuration must contain a valid 'blueprints.graph' setting");

        final Class graphClass;
        try {
            graphClass = Class.forName(clazz);
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(String.format("GraphFactory could not find [%s] - Ensure that the jar is in the classpath", clazz));
        }

        final Graph g;
        try {
            // will basically use Graph.open(Configuration c, Optional<GraphStrategy> s) to instantiate, but could
            // technically use any method on any class with the same signature.
            g = (Graph) graphClass.getMethod("open", Optional.class, Optional.class).invoke(null, Optional.of(configuration), strategy);
        } catch (final NoSuchMethodException e1) {
            throw new RuntimeException(String.format("GraphFactory can only instantiate Graph implementations from classes that have a static open() method that takes a single Apache Commons Configuration argument - [%s] does not seem to have one", clazz));
        } catch (final Exception e2) {
            throw new RuntimeException(String.format("GraphFactory could not instantiate this Graph implementation [%s]", clazz), e2);
        }

        return g;
    }

    public static Graph open(final Configuration configuration) {
        return open(configuration, Optional.empty());
    }

    /**
     * Open a graph.  See each {@link Graph} instance for its configuration options.
     *
     * @param configurationFile The location of a configuration file that specifies the minimally required properties
     *                          for a {@link Graph} instance. This minimum is determined by the {@link Graph} instance
     *                          itself.
     * @return A {@link Graph} instance.
     */
    public static Graph open(final String configurationFile) {
        return open(getConfiguration(new File(configurationFile)));
    }

    /**
     * Open a graph. See each {@link Graph} instance for its configuration options.
     *
     * @param configuration A {@link Map} based configuration that will be converted to an {@link Configuration} object
     *                      via {@link MapConfiguration} and passed to the appropriate overload.
     * @return A Graph instance.
     */
    public static Graph open(final Map configuration) {
        return open(new MapConfiguration(configuration));
    }

    private static Configuration getConfiguration(final File dirOrFile) {
        if (null == dirOrFile)
            throw new IllegalArgumentException("Need to specify a configuration file or storage directory");

        if (!dirOrFile.isFile())
            throw new IllegalArgumentException("Location of configuration must be a file");

        try {
            return new PropertiesConfiguration(dirOrFile);
        } catch (final ConfigurationException e) {
            throw new IllegalArgumentException("Could not load configuration at: " + dirOrFile, e);
        }
    }
}