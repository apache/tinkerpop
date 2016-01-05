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
package org.apache.tinkerpop.gremlin;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * A basic GraphProvider which simply requires the implementer to supply their base configuration for their
 * Graph instance.  Minimally this is just the setting for "gremlin.graph".
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGraphProvider implements GraphProvider {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGraphProvider.class);
    /**
     * Provides a basic configuration for a particular {@link org.apache.tinkerpop.gremlin.structure.Graph} instance and used
     * the {@code graphName} to ensure that the instance is unique.  It is up to the Gremlin implementation
     * to determine how best to use the {@code graphName} to ensure uniqueness.  For example, Neo4j, might use the
     * {@code graphName} might be used to create a different sub-directory where the graph is stored.
     * <p/>
     * The @{code test} and @{code testMethodName} can be used to alter graph configurations for specific tests.
     * For example, a graph that has support for different transaction isolation levels might only support a feature
     * in a specific configuration.  Using these arguments, the implementation could detect when a test was being
     * fired that required the database to be configured in a specific isolation level and return a configuration
     * to support that.
     *
     * @param graphName      a value that represents a unique configuration for a graph
     * @param test           the test class
     * @param testMethodName the name of the test method
     * @param loadGraphWith  the data set to load and will be null if no data is to be loaded
     * @return a configuration {@link java.util.Map} that should be unique per the {@code graphName}
     */
    public abstract Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test,
                                                             final String testMethodName, final LoadGraphWith.GraphData loadGraphWith);

    @Override
    public Configuration newGraphConfiguration(final String graphName, final Class<?> test,
                                               final String testMethodName,
                                               final Map<String, Object> configurationOverrides,
                                               final LoadGraphWith.GraphData loadGraphWith) {
        final Configuration conf = new BaseConfiguration();
        getBaseConfiguration(graphName, test, testMethodName, loadGraphWith).entrySet().stream()
                .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));

        // assign overrides but don't allow gremlin.graph setting to be overridden.  the test suite should
        // not be able to override that.
        configurationOverrides.entrySet().stream()
                .filter(c -> !c.getKey().equals(Graph.GRAPH))
                .forEach(e -> conf.setProperty(e.getKey(), e.getValue()));
        return conf;
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        try {
            // loadGraphWith will be null if an annotation isn't assigned.  it simply means that the graph is
            // created in an ad-hoc manner for the tests - just don't try to read any data into it.
            if (loadGraphWith != null) readIntoGraph(graph, loadGraphWith.value().location());
        } catch (IOException ioe) {
            throw new RuntimeException("Graph could not be loaded with data for test: " + ioe.getMessage());
        }
    }

    /**
     * Helper method for those building {@link GraphProvider} implementations that need to clean directories
     * between test runs.
     */
    protected static void deleteDirectory(final File directory) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
            directory.delete();
        }

        // overkill code, simply allowing us to detect when data dir is in use.  useful though because without it
        // tests may fail if a database is re-used in between tests somehow.  this directory really needs to be
        // cleared between tests runs and this exception will make it clear if it is not. this code used to
        // throw an exception but that fails windows builds in some cases unecessarily - hopefully the print
        // to screen is enough to hint failures due to the old directory still being in place.
        if (directory.exists()) logger.error("unable to delete directory " + directory.getAbsolutePath());
    }

    protected void readIntoGraph(final Graph g, final String path) throws IOException {
        final GraphReader reader = GryoReader.build()
                .mapper(g.io(GryoIo.build()).mapper().create())
                .create();
        try (final InputStream stream = AbstractGremlinTest.class.getResourceAsStream(path)) {
            reader.readGraph(stream, g);
        }
    }
}
