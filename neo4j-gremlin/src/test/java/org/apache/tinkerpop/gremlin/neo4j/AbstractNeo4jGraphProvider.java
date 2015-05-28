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

import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jElement;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jGraphVariables;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jProperty;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import org.apache.tinkerpop.gremlin.neo4j.structure.Neo4jVertexProperty;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.configuration.Configuration;

import java.io.File;
import java.util.*;

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
            if (graph.features().graph().supportsTransactions() && graph.tx().isOpen())
                graph.tx().rollback();
            graph.close();
        }

        if (configuration.containsKey("gremlin.neo4j.directory")) {
            // this is a non-in-sideEffects configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString("gremlin.neo4j.directory"));
            deleteDirectory(graphDirectory);
        }
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (loadGraphWith != null) this.createIndices((Neo4jGraph) graph, loadGraphWith.value());
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

    private void createIndices(final Neo4jGraph g, final LoadGraphWith.GraphData graphData) {
        final Random random = new Random();
        final int pick = random.nextInt(3);
        //final int pick = 2;
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            if (pick == 1) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :artist(name)");
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :song(name)");
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :song(songType)");
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :song(performances)");
                g.tx().commit();
            } else if (pick == 2) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "name");
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "songType");
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "performances");
                g.tx().commit();
            }
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            if (pick == 1) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :person(name)");
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :person(age)");
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :software(name)");
                if (random.nextBoolean()) {
                    createIndex(g, "CREATE INDEX ON :software(lang)");
                }
                g.tx().commit();
            } else if (pick == 2) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "name");
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "age");
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "lang");
                g.tx().commit();
            }
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            if (pick == 1) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :vertex(name)");
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :vertex(age)");
                if (random.nextBoolean())
                    createIndex(g, "CREATE INDEX ON :vertex(lang)");
                g.tx().commit();
            } else if (pick == 2) {
                g.tx().readWrite();
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "name");
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "age");
                if (random.nextBoolean())
                    g.getBaseGraph().autoIndexProperties(true, "lang");
                g.tx().commit();
            }
        } else {
            // TODO: add CREW work here.
            // TODO: add meta_property indices when meta_property graph is provided
            //throw new RuntimeException("Could not load graph with " + graphData);
        }
    }

    private void createIndex(Neo4jGraph g, String indexQuery) {
        Iterator<Map<String, Object>> it = g.getBaseGraph().execute(indexQuery, null);
        while (it.hasNext()) it.next();
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }
}