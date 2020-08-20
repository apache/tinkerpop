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
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.iterator.StoreIteratorCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * Sets up g based on the current graph configuration and checks required features for the test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGremlinTest {
    private static final Logger logger = LoggerFactory.getLogger(AbstractGremlinTest.class);
    protected static final boolean shouldTestIteratorLeak = Boolean.valueOf(System.getProperty("testIteratorLeaks", "true"));
    protected Graph graph;
    protected GraphTraversalSource g;
    protected Configuration config;
    protected GraphProvider graphProvider;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() throws Exception {
        final Method testMethod = this.getClass().getMethod(cleanMethodName(name.getMethodName()));
        final LoadGraphWith[] loadGraphWiths = testMethod.getAnnotationsByType(LoadGraphWith.class);
        final LoadGraphWith loadGraphWith = loadGraphWiths.length == 0 ? null : loadGraphWiths[0];
        final LoadGraphWith.GraphData loadGraphWithData = null == loadGraphWith ? null : loadGraphWith.value();
        final Set<FeatureRequirement> featureRequirementSet = getFeatureRequirementsForTest(testMethod, loadGraphWiths);

        graphProvider = GraphManager.getGraphProvider();

        // pre-check if available from graph provider to avoid graph creation
        final Optional<Graph.Features> staticFeatures = graphProvider.getStaticFeatures();
        if (staticFeatures.isPresent()) {
            assumeRequirementsAreMetForTest(featureRequirementSet, staticFeatures.get(), true);
        }

        graphProvider.getTestListener().ifPresent(l -> l.onTestStart(this.getClass(), name.getMethodName()));

        // Reset the counter for open iterators by this test
        StoreIteratorCounter.INSTANCE.reset();

        config = graphProvider.standardGraphConfiguration(this.getClass(), name.getMethodName(), loadGraphWithData);

        // this should clear state from a previously unfinished test. since the graph does not yet exist,
        // persisted graphs will likely just have their directories removed
        graphProvider.clear(config);

        graph = graphProvider.openTestGraph(config);
        g = graphProvider.traversal(graph);

        // even if we checked static features earlier it's of little cost to recheck again with the real graph
        // once it is instantiated. the real cost savings is preventing graph creation in the first place so
        // let's double check that all is legit.
        assumeRequirementsAreMetForTest(featureRequirementSet, graph.features(), false);

        beforeLoadGraphWith(graph);

        // load a graph with sample data if the annotation is present on the test
        graphProvider.loadGraphData(graph, loadGraphWith, this.getClass(), name.getMethodName());

        afterLoadGraphWith(graph);
    }

    private static void assumeRequirementsAreMetForTest(final Set<FeatureRequirement> featureRequirementSet,
                                                        final Graph.Features features, final boolean staticCheck)
            throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        for (FeatureRequirement fr : featureRequirementSet) {
            try {
                assumeThat(String.format("Features of the graph do not support all of the features required by this test so it will be ignored: %s.%s=%s",
                        fr.featureClass().getSimpleName(), fr.feature(), fr.supported()),
                        features.supports(fr.featureClass(), fr.feature()), is(fr.supported()));
            } catch (NoSuchMethodException nsme) {
                throw new NoSuchMethodException(String.format("[supports%s] is not a valid feature on %s", fr.feature(), fr.featureClass()));
            } catch (UnsupportedOperationException uoe) {
                // no worries if this is a check of static features - it just means that we can't use the cache to
                // support this check and will have to incur the cost of instantiating a graph instance directly. but,
                // if this is not a static check then something else is amiss and we should throw.
                if (staticCheck) throw uoe;
            }
        }
    }

    protected void beforeLoadGraphWith(final Graph g) throws Exception {
        // do nothing
    }

    protected void afterLoadGraphWith(final Graph g) throws Exception {
        // do nothing
    }

    @After
    public void tearDown() throws Exception {
        if (null != graphProvider) {
            graphProvider.getTestListener().ifPresent(l -> l.onTestEnd(this.getClass(), name.getMethodName()));

            // GraphProvider that has implemented the clear method must check null for graph and config.
            // If #assumeRequirementsAreMetForTest returns false in #setup, graph and config will be null.
            graphProvider.clear(graph, config);

            // All GraphProvider objects should be an instance of ManagedGraphProvider, as this is handled by GraphManager
            // which wraps injected GraphProviders with a ManagedGraphProvider instance. If this doesn't happen, there
            // is no way to trace open graphs.
            if(graphProvider instanceof GraphManager.ManagedGraphProvider)
                ((GraphManager.ManagedGraphProvider)graphProvider).tryClearGraphs();
            else
                logger.warn("The {} is not of type ManagedGraphProvider and therefore graph instances may leak between test cases.", graphProvider.getClass());

            if (shouldTestIteratorLeak) {
                final long wait = 300;
                final long[] tries = new long[] { 1, 3, 5, 7, 9, 18, 27, 36, 72, 144, 256, 512 };
                long openItrCount = StoreIteratorCounter.INSTANCE.getOpenIteratorCount();
                for (int ix = 0; ix < tries.length && openItrCount > 0; ix++) {
                    Thread.sleep(wait * tries[ix]);
                    openItrCount = StoreIteratorCounter.INSTANCE.getOpenIteratorCount();
                }

                assertEquals("Iterator leak detected. Open iterator count=" + openItrCount, 0, openItrCount);
            }

            g = null;
            graph = null;
            config = null;
            graphProvider = null;
        }
    }

    /**
     * Looks up the identifier as generated by the current source graph being tested.
     *
     * @param vertexName a unique string that will identify a graph element within a graph
     * @return the id as generated by the graph
     */
    public Object convertToVertexId(final String vertexName) {
        return convertToVertexId(graph, vertexName);
    }

    /**
     * Looks up the identifier as generated by the current source graph being tested.
     *
     * @param graph      the graph to get the element id from
     * @param vertexName a unique string that will identify a graph element within a graph
     * @return the id as generated by the graph
     */
    public Object convertToVertexId(final Graph graph, final String vertexName) {
        return convertToVertex(graph, vertexName).id();
    }

    public Vertex convertToVertex(final String vertexName) {
        return convertToVertex(graph, vertexName);
    }

    public Vertex convertToVertex(final Graph graph, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return graphProvider.traversal(graph).V().has("name", vertexName).toList().get(0);
    }

    public GraphTraversal<Vertex, Object> convertToVertexPropertyId(final String vertexName, final String vertexPropertyKey) {
        return convertToVertexPropertyId(graph, vertexName, vertexPropertyKey);
    }

    public GraphTraversal<Vertex, Object> convertToVertexPropertyId(final Graph graph, final String vertexName, final String vertexPropertyKey) {
        return convertToVertexProperty(graph, vertexName, vertexPropertyKey).id();
    }

    public GraphTraversal<Vertex, VertexProperty<Object>> convertToVertexProperty(final Graph graph, final String vertexName, final String vertexPropertyKey) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return (GraphTraversal<Vertex, VertexProperty<Object>>) graphProvider.traversal(graph).V().has("name", vertexName).properties(vertexPropertyKey);
    }

    public Edge convertToEdge(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
        return graphProvider.traversal(graph).V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").toList().get(0);
    }

    public Object convertToEdgeId(final String outVertexName, String edgeLabel, final String inVertexName) {
        return convertToEdgeId(graph, outVertexName, edgeLabel, inVertexName);
    }

    public Object convertToEdgeId(final Graph graph, final String outVertexName, String edgeLabel, final String inVertexName) {
        return convertToEdge(graph, outVertexName, edgeLabel, inVertexName).id();
    }

    /**
     * Utility method that commits if the graph supports transactions.
     */
    public void tryCommit(final Graph graph) {
        if (graph.features().graph().supportsTransactions())
            graph.tx().commit();
    }

    /**
     * Utility method that commits if the graph supports transactions and executes an assertion function before and
     * after the commit.  It assumes that the assertion should be true before and after the commit.
     */
    public void tryCommit(final Graph graph, final Consumer<Graph> assertFunction) {
        assertFunction.accept(graph);
        if (graph.features().graph().supportsTransactions()) {
            graph.tx().commit();
            assertFunction.accept(graph);
        }
    }

    /**
     * Utility method that rollsback if the graph supports transactions.
     */
    public void tryRollback(final Graph graph) {
        if (graph.features().graph().supportsTransactions())
            graph.tx().rollback();
    }

    /**
     * If using "parameterized test" junit will append an identifier to the end of the method name which prevents it
     * from being found via reflection.  This method removes that suffix.
     */
    private static String cleanMethodName(final String methodName) {
        if (methodName.endsWith("]")) {
            return methodName.substring(0, methodName.indexOf("["));
        }

        return methodName;
    }

    public void printTraversalForm(final Traversal traversal) {
        logger.info(String.format("Testing: %s", name.getMethodName()));
        logger.info("   pre-strategy:" + traversal);
        if (!traversal.asAdmin().isLocked()) traversal.asAdmin().applyStrategies();
        logger.info("  post-strategy:" + traversal);
        verifyUniqueStepIds(traversal.asAdmin());
    }

    public static void assertVertexEdgeCounts(final Graph graph, final int expectedVertexCount, final int expectedEdgeCount) {
        getAssertVertexEdgeCounts(expectedVertexCount, expectedEdgeCount).accept(graph);
    }

    public static Consumer<Graph> getAssertVertexEdgeCounts(final int expectedVertexCount, final int expectedEdgeCount) {
        return (g) -> {
            assertEquals(expectedVertexCount, IteratorUtils.count(g.vertices()));
            assertEquals(expectedEdgeCount, IteratorUtils.count(g.edges()));
        };
    }

    public static void validateException(final Throwable expected, final Throwable actual) {
        assertThat(actual, instanceOf(expected.getClass()));
    }

    public static void verifyUniqueStepIds(final Traversal.Admin<?, ?> traversal) {
        AbstractGremlinTest.verifyUniqueStepIds(traversal, 0, new HashSet<>());
    }

    private static Set<FeatureRequirement> getFeatureRequirementsForTest(final Method testMethod, final LoadGraphWith[] loadGraphWiths) {
        // get feature requirements on the test method and add them to the list of ones to check
        final FeatureRequirement[] featureRequirement = testMethod.getAnnotationsByType(FeatureRequirement.class);
        final List<FeatureRequirement> frs = new ArrayList<>(Arrays.asList(featureRequirement));

        // if the graph is loading data then it will come with it's own requirements
        if (loadGraphWiths.length > 0) frs.addAll(loadGraphWiths[0].value().featuresRequired());

        // if the graph has a set of feature requirements bundled together then add those
        final FeatureRequirementSet[] featureRequirementSets = testMethod.getAnnotationsByType(FeatureRequirementSet.class);
        if (featureRequirementSets.length > 0)
            frs.addAll(Arrays.stream(featureRequirementSets)
                    .flatMap(f -> f.value().featuresRequired().stream()).collect(Collectors.toList()));

        // process the unique set of feature requirements
        return new HashSet<>(frs);
    }

    private static void verifyUniqueStepIds(final Traversal.Admin<?, ?> traversal, final int depth, final Set<String> ids) {
        for (final Step step : traversal.asAdmin().getSteps()) {
            /*for (int i = 0; i < depth; i++) System.out.print("\t");
            System.out.println(step.getId() + " --> " + step);*/
            if (!ids.add(step.getId())) {
                fail("The following step id already exists: " + step.getId() + "---" + step);
            }
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalTraversal : ((TraversalParent) step).getGlobalChildren()) {
                    verifyUniqueStepIds(globalTraversal, depth + 1, ids);
                }
                for (final Traversal.Admin<?, ?> localTraversal : ((TraversalParent) step).getLocalChildren()) {
                    verifyUniqueStepIds(localTraversal, depth + 1, ids);
                }
            }
        }
    }
}
