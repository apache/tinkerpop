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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds objects specified by the test suites supplying them in a static manner to the test cases.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphManager {
    private static GraphProvider graphProvider;
    private static TraversalEngine.Type traversalEngineType;

    public static GraphProvider setGraphProvider(final GraphProvider graphProvider) {
        final GraphProvider old = GraphManager.graphProvider;
        GraphManager.graphProvider = graphProvider;
        return old;
    }

    /**
     * Gets the {@link GraphProvider} from the current test suite and wraps it in a {@link ManagedGraphProvider}.
     */
    public static GraphProvider getGraphProvider() {
        return new ManagedGraphProvider(graphProvider);
    }

    public static TraversalEngine.Type setTraversalEngineType(final TraversalEngine.Type traversalEngine) {
        final TraversalEngine.Type old = GraphManager.traversalEngineType;
        GraphManager.traversalEngineType = traversalEngine;
        return old;
    }

    public static TraversalEngine.Type getTraversalEngineType() {
        return traversalEngineType;
    }

    /**
     * This class provides a way to intercepts calls to {@link Graph} implementation's {@link GraphProvider} instances.
     * When {@link #openTestGraph(Configuration)} is called the created object is stored in a list and when tests are
     * complete the {@link #tryCloseGraphs()} is called. When this is called, an attempt is made to close all open graphs.
     */
    public static class ManagedGraphProvider implements GraphProvider {
        private final GraphProvider innerGraphProvider;
        private final List<Graph> openGraphs = new ArrayList<>();

        public ManagedGraphProvider(final GraphProvider innerGraphProvider){
            this.innerGraphProvider = innerGraphProvider;
        }

        public void tryCloseGraphs(){
            for(Graph graph : openGraphs) {
                try {
                    graph.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        @Override
        public String getWorkingDirectory() {
            return innerGraphProvider.getWorkingDirectory();
        }

        @Override
        public GraphTraversalSource traversal(final Graph graph) {
            return innerGraphProvider.traversal(graph);
        }

        @Override
        public GraphTraversalSource traversal(final Graph graph, final TraversalStrategy... strategies) {
            return innerGraphProvider.traversal(graph, strategies);
        }

        @Override
        public Graph standardTestGraph(final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
            final Graph graph = innerGraphProvider.standardTestGraph(test, testMethodName, loadGraphWith);
            openGraphs.add(graph);
            return graph;
        }

        @Override
        public Graph openTestGraph(final Configuration config) {
            final Graph graph = innerGraphProvider.openTestGraph(config);
            openGraphs.add(graph);
            return graph;
        }

        @Override
        public Configuration standardGraphConfiguration(final Class<?> test, final String testMethodName, final LoadGraphWith.GraphData loadGraphWith) {
            return innerGraphProvider.standardGraphConfiguration(test, testMethodName, loadGraphWith);
        }

        @Override
        public void clear(final Configuration configuration) throws Exception {
            innerGraphProvider.clear(configuration);
        }

        @Override
        public void clear(final Graph graph, final Configuration configuration) throws Exception {
            innerGraphProvider.clear(graph, configuration);
        }

        @Override
        public Object convertId(final Object id, final Class<? extends Element> c) {
            return innerGraphProvider.convertId(id, c);
        }

        @Override
        public String convertLabel(final String label) {
            return innerGraphProvider.convertLabel(label);
        }

        @Override
        public Configuration newGraphConfiguration(final String graphName, final Class<?> test, final String testMethodName,
                                                   final Map<String, Object> configurationOverrides, final LoadGraphWith.GraphData loadGraphWith) {
            return innerGraphProvider.newGraphConfiguration(graphName, test, testMethodName, configurationOverrides, loadGraphWith);
        }

        @Override
        public Configuration newGraphConfiguration(final String graphName, final Class<?> test, final String testMethodName,final LoadGraphWith.GraphData loadGraphWith) {
            return innerGraphProvider.newGraphConfiguration(graphName, test, testMethodName, loadGraphWith);
        }

        @Override
        public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
            innerGraphProvider.loadGraphData(graph, loadGraphWith, testClass, testName);
        }

        @Override
        public Set<Class> getImplementations() {
            return innerGraphProvider.getImplementations();
        }
    }

}
