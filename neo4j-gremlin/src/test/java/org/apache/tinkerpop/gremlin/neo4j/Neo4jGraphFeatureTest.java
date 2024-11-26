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
package org.apache.tinkerpop.gremlin.neo4j;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.cucumber.guice.CucumberModules;
import io.cucumber.guice.GuiceFactory;
import io.cucumber.guice.InjectorSource;
import io.cucumber.java.Scenario;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;

@RunWith(Cucumber.class)
@CucumberOptions(
        tags = "not @RemoteOnly and not @MultiProperties and not @MetaProperties and not @GraphComputerOnly and not @AllowNullPropertyValues and not @UserSuppliedVertexPropertyIds and not @UserSuppliedEdgeIds and not @UserSuppliedVertexIds and not @TinkerServiceRegistry and not @StepHasId",
        glue = { "org.apache.tinkerpop.gremlin.features" },
        objectFactory = GuiceFactory.class,
        features = { "classpath:/org/apache/tinkerpop/gremlin/test/features" },
        plugin = {"progress", "junit:target/cucumber.xml"})
public class Neo4jGraphFeatureTest {
    private static final Logger logger = LoggerFactory.getLogger(Neo4jGraphFeatureTest.class);

    public static final class ServiceModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(World.class).to(Neo4jGraphWorld.class);
        }
    }

    public static class Neo4jGraphWorld implements World {

        private static final Neo4jGraph modern = Neo4jGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.MODERN)));
        private static final Neo4jGraph classic = Neo4jGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.CLASSIC)));
        private static final Neo4jGraph sink = Neo4jGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.SINK)));
        private static final Neo4jGraph grateful = Neo4jGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.GRATEFUL)));
        private static final Neo4jGraph empty = Neo4jGraph.open(new MapConfiguration(getBaseConfiguration(null)));

        static {
            createIndices();
            readIntoGraph(modern, GraphData.MODERN);
            readIntoGraph(classic, GraphData.CLASSIC);
            readIntoGraph(sink, GraphData.SINK);
            readIntoGraph(grateful, GraphData.GRATEFUL);
        }

        @Override
        public GraphTraversalSource getGraphTraversalSource(final GraphData graphData) {
            if (null == graphData)
                return empty.traversal();
            else if (graphData == GraphData.CLASSIC)
                return classic.traversal();
            else if (graphData == GraphData.CREW)
                throw new UnsupportedOperationException("The Crew dataset is not supported by Neo4j because it doesn't support mult/meta-properties");
            else if (graphData == GraphData.MODERN)
                return modern.traversal();
            else if (graphData == GraphData.SINK)
                return sink.traversal();
            else if (graphData == GraphData.GRATEFUL)
                return grateful.traversal();
            else
                throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
        }

        @Override
        public void beforeEachScenario(final Scenario scenario) {
            cleanEmpty();
        }

        @Override
        public String changePathToDataFile(final String pathToFileFromGremlin) {
            return ".." + File.separator + pathToFileFromGremlin;
        }

        private static void createIndices() {
            grateful.tx().readWrite();
            grateful.cypher("CREATE INDEX ON :artist(name)").iterate();
            grateful.cypher("CREATE INDEX ON :song(name)").iterate();
            grateful.cypher("CREATE INDEX ON :song(songType)").iterate();
            grateful.cypher("CREATE INDEX ON :song(performances)").iterate();
            grateful.tx().commit();

            modern.tx().readWrite();
            modern.cypher("CREATE INDEX ON :person(name)").iterate();
            modern.cypher("CREATE INDEX ON :person(age)").iterate();
            modern.cypher("CREATE INDEX ON :software(name)").iterate();
            modern.cypher("CREATE INDEX ON :software(lang)").iterate();
            modern.tx().commit();

            classic.tx().readWrite();
            classic.cypher("CREATE INDEX ON :vertex(name)").iterate();
            classic.cypher("CREATE INDEX ON :vertex(age)").iterate();
            classic.cypher("CREATE INDEX ON :vertex(lang)").iterate();
            classic.tx().commit();
        }

        private void cleanEmpty() {
            final GraphTraversalSource g = empty.traversal();
            g.V().drop().iterate();
        }

        private static void readIntoGraph(final Graph graph, final GraphData graphData) {
            try {
                final String dataFile = TestHelper.generateTempFileFromResource(graph.getClass(),
                        GryoResourceAccess.class, graphData.location().substring(graphData.location().lastIndexOf(File.separator) + 1), "", false).getAbsolutePath();
                graph.traversal().io(dataFile).read().iterate();
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }

        private static String getWorkingDirectory() {
            return TestHelper.makeTestDataDirectory(Neo4jGraphFeatureTest.class, "graph-provider-data");
        }

        private static String makeTestDirectory(final String graphName) {
            return getWorkingDirectory() + File.separator
                    + RandomStringUtils.randomAlphabetic(10) + File.separator
                    + TestHelper.cleanPathSegment(graphName);
        }

        private static Map<String, Object> getBaseConfiguration(final LoadGraphWith.GraphData graphData) {
            final String directory = makeTestDirectory(graphData == null ? "default" : graphData.name().toLowerCase());

            final File f = new File(directory);
            if (f.exists()) deleteDirectory(f);
            f.mkdirs();

            return new HashMap<String, Object>() {{
                put(Graph.GRAPH, Neo4jGraph.class.getName());
                put(Neo4jGraph.CONFIG_DIRECTORY, directory);
                put(Neo4jGraph.CONFIG_CONF + ".dbms.memory.pagecache.size", "1m");
            }};
        }

        static void deleteDirectory(final File directory) {
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

            if (directory.exists()) logger.error("unable to delete directory " + directory.getAbsolutePath());
        }
    }

    public static final class WorldInjectorSource implements InjectorSource {
        @Override
        public Injector getInjector() {
            return Guice.createInjector(Stage.PRODUCTION, CucumberModules.createScenarioModule(), new ServiceModule());
        }
    }

}
