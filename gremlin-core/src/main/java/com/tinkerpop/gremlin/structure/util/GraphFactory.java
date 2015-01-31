package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.config.YamlConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import java.io.File;
import java.util.Map;

/**
 * Factory to construct new {@link com.tinkerpop.gremlin.structure.Graph} instances from a
 * {@link org.apache.commons.configuration.Configuration} object or properties file.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphFactory {

    /**
     * Open a graph.  See each {@link com.tinkerpop.gremlin.structure.Graph} instance for its configuration options.
     *
     * @param configuration A configuration object that specifies the minimally required properties for a                        I
     *                      {@link com.tinkerpop.gremlin.structure.Graph} instance. This minimum is determined by the
     *                      {@link com.tinkerpop.gremlin.structure.Graph} instance itself.
     * @return A {@link com.tinkerpop.gremlin.structure.Graph} instance.
     * @throws IllegalArgumentException if {@code configuration}
     */
    public static Graph open(final Configuration configuration) {
        if (null == configuration)
            throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        final String clazz = configuration.getString(Graph.GRAPH, null);
        if (null == clazz)
            throw new RuntimeException("Configuration must contain a valid 'gremlin.graph' setting");

        final Class graphClass;
        try {
            graphClass = Class.forName(clazz);
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(String.format("GraphFactory could not find [%s] - Ensure that the jar is in the classpath", clazz));
        }

        final Graph g;
        try {
            // will basically use Graph.open(Configuration c) to instantiate, but could
            // technically use any method on any class with the same signature.
            g = (Graph) graphClass.getMethod("open", Configuration.class).invoke(null, configuration);
        } catch (final NoSuchMethodException e1) {
            throw new RuntimeException(String.format("GraphFactory can only instantiate Graph implementations from classes that have a static open() method that takes a single Apache Commons Configuration argument - [%s] does not seem to have one", clazz));
        } catch (final Exception e2) {
            throw new RuntimeException(String.format("GraphFactory could not instantiate this Graph implementation [%s]", clazz), e2);
        }

        return g;
    }

    /**
     * Open a graph.  See each {@link com.tinkerpop.gremlin.structure.Graph} instance for its configuration options. This file may be XML, YAML,
     * or a standard properties file. How the configuration is used (and which kind is required) is dependent on
     * the implementation.
     *
     * @param configurationFile The location of a configuration file that specifies the minimally required properties
     *                          for a {@link com.tinkerpop.gremlin.structure.Graph} instance. This minimum is determined by the {@link com.tinkerpop.gremlin.structure.Graph} instance
     *                          itself.
     * @return A {@link com.tinkerpop.gremlin.structure.Graph} instance.
     * @throws IllegalArgumentException if {@code configurationFile} is null
     */
    public static Graph open(final String configurationFile) {
        if (null == configurationFile) throw Graph.Exceptions.argumentCanNotBeNull("configurationFile");
        return open(getConfiguration(new File(configurationFile)));
    }

    /**
     * Open a graph. See each {@link com.tinkerpop.gremlin.structure.Graph} instance for its configuration options.
     *
     * @param configuration A {@link java.util.Map} based configuration that will be converted to an {@link org.apache.commons.configuration.Configuration} object
     *                      via {@link org.apache.commons.configuration.MapConfiguration} and passed to the appropriate overload.
     * @return A Graph instance.
     */
    public static Graph open(final Map configuration) {
        return open(new MapConfiguration(configuration));
    }

    private static Configuration getConfiguration(final File configurationFile) {
        if (null == configurationFile)
            throw Graph.Exceptions.argumentCanNotBeNull("configurationFile");

        if (!configurationFile.isFile())
            throw new IllegalArgumentException(String.format("The location configuration must resolve to a file and [%s] does not", configurationFile));

        try {
            final String fileName = configurationFile.getName();
            final String fileExtension = fileName.substring(fileName.lastIndexOf(".") + 1);

            switch (fileExtension) {
                case "yml":
                case "yaml":
                    return new YamlConfiguration(configurationFile);
                case "xml":
                    return new XMLConfiguration(configurationFile);
                default:
                    return new PropertiesConfiguration(configurationFile);
            }
        } catch (final ConfigurationException e) {
            throw new IllegalArgumentException(String.format("Could not load configuration at: %s", configurationFile), e);
        }
    }
}