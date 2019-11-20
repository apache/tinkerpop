/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

import java.io.File;
import java.util.Map;

/**
 * Factory to construct new {@link Graph} instances from a {@code Configuration} object or properties file.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphFactory {

    private GraphFactory() {}

    /**
     * Open a graph.  See each {@link Graph} instance for its configuration options.
     *
     * @param configuration A configuration object that specifies the minimally required properties for a
     *                      {@link Graph} instance. This minimum is determined by the
     *                      {@link Graph} instance itself.
     * @return A {@link Graph} instance.
     * @throws IllegalArgumentException if {@code configuration}
     */
    public static Graph open(final Configuration configuration) {
        if (null == configuration)
            throw Graph.Exceptions.argumentCanNotBeNull("configuration");

        final String clazz = configuration.getString(Graph.GRAPH, null);
        if (null == clazz)
            throw new RuntimeException(String.format("Configuration must contain a valid '%s' setting", Graph.GRAPH));

        final Class<?> graphClass;
        try {
            graphClass = Class.forName(clazz);
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(String.format("GraphFactory could not find [%s] - Ensure that the jar is in the classpath", clazz));
        }

        // If the graph class specifies a factory class then use that instead of the specified class.
        final GraphFactoryClass factoryAnnotation = graphClass.getAnnotation(GraphFactoryClass.class);
        final Class<?> factoryClass = factoryAnnotation != null ? factoryAnnotation.value() : graphClass;

        return open(configuration, factoryClass);
    }

    private static Graph open(final Configuration configuration, final Class<?> graphFactoryClass)
    {
        final Graph g;
        try {
            // will use open(Configuration c) to instantiate
            g = (Graph) graphFactoryClass.getMethod("open", Configuration.class).invoke(null, configuration);
        } catch (final NoSuchMethodException e1) {
            throw new RuntimeException(String.format("GraphFactory can only instantiate Graph implementations from classes that have a static open() method that takes a single Apache Commons Configuration argument - [%s] does not seem to have one", graphFactoryClass));
        } catch (final Exception e2) {
            throw new RuntimeException(String.format("GraphFactory could not instantiate this Graph implementation [%s]", graphFactoryClass), e2);
        }
        return g;
    }

    /**
     * Open a graph.  See each {@link Graph} instance for its configuration options. This file may be XML, YAML,
     * or a standard properties file. How the configuration is used (and which kind is required) is dependent on
     * the implementation.
     * <p/>
     * If using XML, ensure that the appropriate version of Apache {@code commons-collections} is available on the
     * classpath as it is an optional dependency of Apache {@code commons-configuration}, the library that
     * {@code GraphFactory} depends on.
     *
     * @param configurationFile The location of a configuration file that specifies the minimally required properties
     *                          for a {@link Graph} instance. This minimum is determined by the {@link Graph} instance
     *                          itself.
     * @return A {@link Graph} instance.
     * @throws IllegalArgumentException if {@code configurationFile} is null
     */
    public static Graph open(final String configurationFile) {
        if (null == configurationFile) throw Graph.Exceptions.argumentCanNotBeNull("configurationFile");
        return open(getConfiguration(new File(configurationFile)));
    }

    /**
     * Open a graph. See each {@link Graph} instance for its configuration options.
     *
     * @param configuration A {@code Map} based configuration that will be converted to an {@code Configuration} object
     *                      via {@code MapConfiguration} and passed to the appropriate overload.
     * @return A Graph instance.
     */
    public static Graph open(final Map configuration) {
        if (null == configuration) throw Graph.Exceptions.argumentCanNotBeNull("configuration");
        return open(new MapConfiguration(configuration));
    }

    private static Configuration getConfiguration(final File configurationFile) {
        if (!configurationFile.isFile())
            throw new IllegalArgumentException(String.format("The location configuration must resolve to a file and [%s] does not", configurationFile));

        try {
            final String fileName = configurationFile.getName();
            final String fileExtension = fileName.substring(fileName.lastIndexOf('.') + 1);

            switch (fileExtension) {
                case "yml":
                case "yaml":
                    final YamlConfiguration config = new YamlConfiguration();
                    config.load(configurationFile);
                    return config;
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