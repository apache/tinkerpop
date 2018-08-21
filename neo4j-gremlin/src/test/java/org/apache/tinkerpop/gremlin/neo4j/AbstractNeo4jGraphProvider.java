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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jElement;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraphVariables;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jProperty;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractNeo4jGraphProvider extends AbstractGraphProvider {
    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(Neo4jEdge.class);
        add(Neo4jElement.class);
        add(Neo4jGraph.class);
        add(Neo4jGraphVariables.class);
        add(Neo4jProperty.class);
        add(Neo4jVertex.class);
        add(Neo4jVertexProperty.class);
    }};

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        if (null != graph) {
            if (graph.tx().isOpen()) graph.tx().rollback();
            graph.close();
        }

        if (null != configuration && configuration.containsKey(Neo4jGraph.CONFIG_DIRECTORY)) {
            // this is a non-in-sideEffects configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString(Neo4jGraph.CONFIG_DIRECTORY));
            deleteDirectory(graphDirectory);
        }
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (loadGraphWith != null) this.createIndices((Neo4jGraph) graph, loadGraphWith.value());
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

    public static void dropIndices(final Neo4jGraph graph, final LoadGraphWith.GraphData graphData) {
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            graph.tx().readWrite();
            graph.cypher("DROP INDEX ON :artist(name)").iterate();
            graph.cypher("DROP INDEX ON :song(name)").iterate();
            graph.cypher("DROP INDEX ON :song(songType)").iterate();
            graph.cypher("DROP INDEX ON :song(performances)").iterate();
            graph.tx().commit();
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            graph.tx().readWrite();
            graph.cypher("DROP INDEX ON :person(name)").iterate();
            graph.cypher("DROP INDEX ON :person(age)").iterate();
            graph.cypher("DROP INDEX ON :software(name)").iterate();
            graph.cypher("DROP INDEX ON :software(lang)").iterate();
            graph.tx().commit();
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            graph.tx().readWrite();
            graph.cypher("DROP INDEX ON :vertex(name)").iterate();
            graph.cypher("DROP INDEX ON :vertex(age)").iterate();
            graph.cypher("DROP INDEX ON :vertex(lang)").iterate();
            graph.tx().commit();
        } else {
            // TODO: add CREW work here.
            // TODO: add meta_property indices when meta_property graph is provided
            //throw new RuntimeException("Could not load graph with " + graphData);
        }
    }

    private void createIndices(final Neo4jGraph graph, final LoadGraphWith.GraphData graphData) {
        final Random random = TestHelper.RANDOM;
        final boolean pick = random.nextBoolean();
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            if (pick) {
                graph.tx().readWrite();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :artist(name)").iterate();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :song(name)").iterate();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :song(songType)").iterate();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :song(performances)").iterate();
                graph.tx().commit();
            } // else no indices
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            if (pick) {
                graph.tx().readWrite();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :person(name)").iterate();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :person(age)").iterate();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :software(name)").iterate();
                if (random.nextBoolean()) {
                    graph.cypher("CREATE INDEX ON :software(lang)").iterate();
                }
                graph.tx().commit();
            } // else no indices
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            if (pick) {
                graph.tx().readWrite();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :vertex(name)").iterate();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :vertex(age)").iterate();
                if (random.nextBoolean())
                    graph.cypher("CREATE INDEX ON :vertex(lang)").iterate();
                graph.tx().commit();
            } // else no indices
        } else {
            // TODO: add CREW work here.
            // TODO: add meta_property indices when meta_property graph is provided
            //throw new RuntimeException("Could not load graph with " + graphData);
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }
}