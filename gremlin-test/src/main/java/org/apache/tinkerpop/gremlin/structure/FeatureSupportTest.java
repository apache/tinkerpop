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
package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.FeatureRequirementSet;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.*;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.*;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeThat;

/**
 * Tests that do basic validation of proper Feature settings in Graph implementations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class FeatureSupportTest {
    private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";
    private static final Logger logger = LoggerFactory.getLogger(FeatureSupportTest.class);

    public static class FeatureToStringTest extends AbstractGremlinTest {
        /**
         * A rough test to ensure that StringFactory is being used to toString Features.
         */
        @Test
        public void shouldHaveStandardToStringRepresentation() {
            assertTrue(graph.features().toString().startsWith("FEATURES"));
        }
    }

    /**
     * Feature checks that test {@link org.apache.tinkerpop.gremlin.structure.Graph} functionality to determine if a feature should be on when it is marked
     * as not supported.
     */
    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "variablesNotSupported",
            "graphComputerNotSupported",
            "transactionsNotSupported"
    })
    public static class GraphFunctionalityTest extends AbstractGremlinTest {

        /**
         * This isn't really a test.  It just pretty prints the features for the graph for reference.  Of course,
         * if the implementing classes use anonymous inner classes it will
         */
        @Test
        public void shouldPrintTheFeatureList() {
            logger.info(String.format("Printing Features of %s for reference: ", g.getClass().getSimpleName()));
            logger.info(graph.features().toString());
            assertTrue(true);
        }

        /**
         * A {@link org.apache.tinkerpop.gremlin.structure.Graph} that does not support {@link org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_COMPUTER} must call
         * {@link org.apache.tinkerpop.gremlin.structure.Graph.Exceptions#graphComputerNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_COMPUTER, supported = false)
        public void shouldSupportComputerIfAGraphCanCompute() throws Exception {
            try {
                graph.compute();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_COMPUTER));
            } catch (Exception e) {
                validateException(Graph.Exceptions.graphComputerNotSupported(), e);
            }
        }

        /**
         * A {@link org.apache.tinkerpop.gremlin.structure.Graph} that does not support {@link org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_TRANSACTIONS} must call
         * {@link org.apache.tinkerpop.gremlin.structure.Graph.Exceptions#transactionsNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_TRANSACTIONS, supported = false)
        public void shouldSupportTransactionsIfAGraphConstructsATx() throws Exception {
            try {
                graph.tx();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_TRANSACTIONS));
            } catch (Exception e) {
                validateException(Graph.Exceptions.transactionsNotSupported(), e);
            }
        }

        /**
         * A {@link org.apache.tinkerpop.gremlin.structure.Graph} that does not support {@link org.apache.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures#FEATURE_VARIABLES} must call
         * {@link org.apache.tinkerpop.gremlin.structure.Graph.Exceptions#variablesNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES, supported = false)
        public void shouldSupportMemoryIfAGraphAcceptsMemory() throws Exception {
            try {
                graph.variables();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, Graph.Features.VariableFeatures.class.getSimpleName(), FEATURE_VARIABLES));
            } catch (Exception e) {
                validateException(Graph.Exceptions.variablesNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_THREADED_TRANSACTIONS, supported = false)
        public void shouldThrowOnThreadedTransactionNotSupported() {
            try {
                graph.tx().createThreadedTx();
                fail("An exception should be thrown since the threaded transaction feature is not supported");
            } catch (Exception e) {
                validateException(Transaction.Exceptions.threadedTransactionsNotSupported(), e);
            }
        }
    }

    /**
     * Feature checks that test {@link org.apache.tinkerpop.gremlin.structure.Vertex} functionality to determine if a feature
     * should be on when it is marked as not supported.
     */
    @ExceptionCoverage(exceptionClass = Vertex.Exceptions.class, methods = {
            "userSuppliedIdsNotSupported",
            "userSuppliedIdsOfThisTypeNotSupported",
            "vertexRemovalNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "vertexAdditionsNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "propertyAdditionNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyRemovalNotSupported"
    })
    public static class VertexFunctionalityTest extends AbstractGremlinTest {

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES, supported = false)
        public void shouldSupportAddVerticesIfAVertexCanBeAdded() throws Exception {
            try {
                graph.addVertex();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexFeatures.FEATURE_ADD_VERTICES));
            } catch (Exception e) {
                validateException(Graph.Exceptions.vertexAdditionsNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsIfAnIdCanBeAssignedToVertex() throws Exception {
            try {
                graph.addVertex(T.id, graphProvider.convertId(99999943835l, Vertex.class));
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_USER_SUPPLIED_IDS));
            } catch (Exception e) {
                validateException(Vertex.Exceptions.userSuppliedIdsNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldNotAllowAnyIdsIfUserSuppliedIdsIsFalse() throws Exception {
            assertFalse(graph.features().vertex().willAllowId(graphProvider.convertId(99999943835l, Vertex.class)));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_STRING_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeString() throws Exception {
            final String id = "this-is-a-valid-id";

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().willAllowId(id));

            try {
                graph.addVertex(T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_STRING_IDS));
            } catch (Exception e) {
                validateException(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeNumericInt() throws Exception {
            final int id = 123456;

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().willAllowId(id));

            try {
                graph.addVertex(T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
            } catch (Exception e) {
                validateException(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeNumericLong() throws Exception {
            final long id = 123456l;

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().willAllowId(id));

            try {
                graph.addVertex(T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
            } catch (Exception e) {
                validateException(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_UUID_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeUuid() throws Exception {
            final UUID id = UUID.randomUUID();

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().willAllowId(id));

            try {
                graph.addVertex(T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_UUID_IDS));
            } catch (Exception e) {
                validateException(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_ANY_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeAny() throws Exception {
            try {
                final Date id = new Date();
                graph.addVertex(T.id, id);

                // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
                // to throw the exception
                if (!graph.features().vertex().willAllowId(id))
                    fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_ANY_IDS));
            } catch (Exception e) {
                validateException(Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_STRING_IDS, supported = false)
        public void shouldSupportStringIdsIfStringIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            if (v.id() instanceof String)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_STRING_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_UUID_IDS, supported = false)
        public void shouldSupportUuidIdsIfUuidIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            if (v.id() instanceof UUID)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_UUID_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportNumericIdsIfNumericIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            if (v.id() instanceof Number)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = VertexFeatures.FEATURE_ADD_PROPERTY, supported = false)
        public void shouldSupportAddVertexPropertyIfItCanBeAdded() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                v.property(VertexProperty.Cardinality.single, "should", "not-add-property");
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexFeatures.FEATURE_ADD_PROPERTY));
            } catch (Exception e) {
                validateException(Element.Exceptions.propertyAdditionNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_REMOVE_VERTICES, supported = false)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSupportRemoveVerticesIfAVertexCanBeRemoved() throws Exception {
            try {
                graph.addVertex().remove();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexFeatures.FEATURE_REMOVE_VERTICES));
            } catch (Exception e) {
                validateException(Vertex.Exceptions.vertexRemovalNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = VertexFeatures.FEATURE_REMOVE_PROPERTY, supported = false)
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldSupportRemovePropertyIfAPropertyCanBeRemoved() throws Exception {
            try {
                final Vertex v = graph.addVertex("name", "me");
                v.property("name").remove();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexFeatures.FEATURE_REMOVE_PROPERTY));
            } catch (Exception e) {
                validateException(Property.Exceptions.propertyRemovalNotSupported(), e);
            }
        }
    }

    /**
     * Feature checks that test {@link org.apache.tinkerpop.gremlin.structure.Edge} functionality to determine if a feature
     * should be on when it is marked as not supported.
     */
    @ExceptionCoverage(exceptionClass = Vertex.Exceptions.class, methods = {
            "edgeAdditionsNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Edge.Exceptions.class, methods = {
            "edgeRemovalNotSupported",
            "userSuppliedIdsNotSupported",
            "userSuppliedIdsOfThisTypeNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "propertyAdditionNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyRemovalNotSupported"
    })
    public static class EdgeFunctionalityTest extends AbstractGremlinTest {

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES, supported = false)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSupportAddEdgesIfEdgeCanBeAdded() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                v.addEdge("friend", v);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), EdgeFeatures.FEATURE_ADD_EDGES));
            } catch (Exception e) {
                validateException(Vertex.Exceptions.edgeAdditionsNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = EdgeFeatures.FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsIfAnIdCanBeAssignedToEdge() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                v.addEdge("friend", v, T.id, graphProvider.convertId(99999943835l, Edge.class));
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), EdgeFeatures.FEATURE_USER_SUPPLIED_IDS));
            } catch (Exception e) {
                validateException(Edge.Exceptions.userSuppliedIdsNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = EdgeFeatures.FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldNotAllowAnyIdsIfUserSuppliedIdsIsFalse() throws Exception {
            assertFalse(graph.features().edge().willAllowId(graphProvider.convertId(99999943835l, Edge.class)));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_STRING_IDS, supported = false)
        public void shouldSupportStringIdsIfStringIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v);
            if (e.id() instanceof String)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_STRING_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_UUID_IDS, supported = false)
        public void shouldSupportUuidIdsIfUuidIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v);
            if (e.id() instanceof UUID)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_UUID_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportNumericIdsIfNumericIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            final Edge e = v.addEdge("knows", v);
            if (e.id() instanceof Number)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_STRING_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeString() throws Exception {
            final String id = "this-is-a-valid-id";

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().edge().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.addEdge("test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_STRING_IDS));
            } catch (Exception e) {
                validateException(Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeNumericInt() throws Exception {
            final int id = 123456;

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().edge().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.addEdge("test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
            } catch (Exception e) {
                validateException(Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeNumericLong() throws Exception {
            final long id = 123456l;

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().edge().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.addEdge("test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
            } catch (Exception e) {
                validateException(Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_UUID_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeUuid() throws Exception {
            final UUID id = UUID.randomUUID();

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().edge().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.addEdge("test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_UUID_IDS));
            } catch (Exception e) {
                validateException(Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_ANY_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeAny() throws Exception {
            final Date id = new Date();

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().edge().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.addEdge("test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_ANY_IDS));
            } catch (Exception e) {
                validateException(Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = EdgeFeatures.FEATURE_ADD_PROPERTY, supported = false)
        public void shouldSupportAddVertexPropertyIfItCanBeAdded() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                final Edge e = v.addEdge("test", v);
                e.property("should", "not-add-property");
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgePropertyFeatures.class.getSimpleName(), EdgeFeatures.FEATURE_ADD_PROPERTY));
            } catch (Exception e) {
                validateException(Element.Exceptions.propertyAdditionNotSupported(), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_REMOVE_EDGES, supported = false)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldSupportRemoveEdgesIfEdgeCanBeRemoved() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                v.addEdge("friend", v);
                v.remove();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), EdgeFeatures.FEATURE_REMOVE_EDGES));
            } catch (Exception ex) {
                validateException(Edge.Exceptions.edgeRemovalNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = EdgeFeatures.FEATURE_REMOVE_PROPERTY, supported = false)
        @FeatureRequirementSet(FeatureRequirementSet.Package.SIMPLE)
        public void shouldSupportRemovePropertyIfAPropertyCanBeRemoved() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                final Edge e = v.addEdge("self", v);
                e.property("name").remove();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), EdgeFeatures.FEATURE_REMOVE_PROPERTY));
            } catch (Exception ex) {
                validateException(Property.Exceptions.propertyRemovalNotSupported(), ex);
            }
        }
    }

    /**
     * Feature checks that test {@link org.apache.tinkerpop.gremlin.structure.Element} {@link org.apache.tinkerpop.gremlin.structure.Property} functionality to determine if a feature should be on
     * when it is marked as not supported.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "dataTypeOfPropertyValueNotSupported"
    })
    public static class ElementPropertyDataTypeFunctionalityTest extends AbstractGremlinTest {
        private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

        @Parameterized.Parameters(name = "supports{0}({1})")
        public static Iterable<Object[]> data() {
            return PropertyTest.PropertyFeatureSupportTest.data();
        }

        @Parameterized.Parameter(value = 0)
        public String featureName;

        @Parameterized.Parameter(value = 1)
        public Object value;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = EdgeFeatures.FEATURE_ADD_PROPERTY)
        public void shouldEnableFeatureOnEdgeIfNotEnabled() throws Exception {
            assumeThat(graph.features().supports(EdgePropertyFeatures.class, featureName), is(false));
            try {
                final Edge edge = createEdgeForPropertyFeatureTests();
                edge.property("aKey", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgePropertyFeatures.class.getSimpleName(), featureName));
            } catch (Exception e) {
                validateException(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value), e);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldEnableFeatureOnVertexIfNotEnabled() throws Exception {
            assumeThat(graph.features().supports(VertexPropertyFeatures.class, featureName), is(false));
            try {
                graph.addVertex("aKey", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), featureName));
            } catch (Exception e) {
                validateException(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value), e);
            }
        }

        private Edge createEdgeForPropertyFeatureTests() {
            final Vertex vertexA = graph.addVertex();
            final Vertex vertexB = graph.addVertex();
            return vertexA.addEdge(graphProvider.convertLabel("knows"), vertexB);
        }
    }

    /**
     * Feature checks that tests if {@link org.apache.tinkerpop.gremlin.structure.Graph.Variables}
     * functionality to determine if a feature should be on when it is marked as not supported.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Graph.Variables.Exceptions.class, methods = {
            "dataTypeOfVariableValueNotSupported"
    })
    public static class GraphVariablesFunctionalityTest extends AbstractGremlinTest {
        private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

        @Parameterized.Parameters(name = "supports{0}({1})")
        public static Iterable<Object[]> data() {
            return VariablesTest.GraphVariablesFeatureSupportTest.data();
        }

        @Parameterized.Parameter(value = 0)
        public String featureName;

        @Parameterized.Parameter(value = 1)
        public Object value;

        /**
         * In this case, the feature requirement for sideEffects is checked, because it means that at least one aspect of
         * sideEffects is supported so we need to test other features to make sure they work properly.
         */
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = Graph.Features.VariableFeatures.FEATURE_VARIABLES)
        public void shouldEnableFeatureOnGraphIfNotEnabled() throws Exception {
            assumeThat(graph.features().supports(Graph.Features.VariableFeatures.class, featureName), is(false));
            try {
                final Graph.Variables variables = graph.variables();
                variables.set("aKey", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, Graph.Features.VariableFeatures.class.getSimpleName(), featureName));
            } catch (Exception e) {
                validateException(Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value), e);
            }
        }
    }

    @ExceptionCoverage(exceptionClass = VertexProperty.Exceptions.class, methods = {
            "multiPropertiesNotSupported",
            "metaPropertiesNotSupported",
            "userSuppliedIdsNotSupported",
            "userSuppliedIdsOfThisTypeNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "propertyRemovalNotSupported"
    })
    public static class VertexPropertyFunctionalityTest extends AbstractGremlinTest {

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsIfAnIdCanBeAssigned() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                v.property(VertexProperty.Cardinality.single, "name", "me", T.id, graphProvider.convertId(99999943835l, VertexProperty.class));
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.userSuppliedIdsNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldNotAllowAnyIdsIfUserSuppliedIdsIsFalse() throws Exception {
            assertFalse(graph.features().vertex().properties().willAllowId(graphProvider.convertId(99999943835l, VertexProperty.class)));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeString() throws Exception {
            final String id = "this-is-a-valid-id";

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().properties().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.property(VertexProperty.Cardinality.single, "test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), FEATURE_STRING_IDS));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeNumericInt() throws Exception {
            final int id = 123456;

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().properties().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.property(VertexProperty.Cardinality.single, "test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeNumericLong() throws Exception {
            final long id = 123456l;

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().properties().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.property(VertexProperty.Cardinality.single, "test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_UUID_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeUuid() throws Exception {
            final UUID id = UUID.randomUUID();

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().properties().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.property(VertexProperty.Cardinality.single, "test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), FEATURE_UUID_IDS));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_ANY_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsOfTypeAny() throws Exception {
            final Date id = new Date();

            // a graph can "allow" an id without internally supporting it natively and therefore doesn't need
            // to throw the exception
            assumeFalse(graph.features().vertex().properties().willAllowId(id));

            try {
                final Vertex v = graph.addVertex();
                v.property(VertexProperty.Cardinality.single, "test", v, T.id, id);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), FEATURE_ANY_IDS));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_STRING_IDS, supported = false)
        public void shouldSupportStringIdsIfStringIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            final VertexProperty p = v.property(VertexProperty.Cardinality.single, "name", "stephen");
            if (p.id() instanceof String)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), VertexPropertyFeatures.FEATURE_STRING_IDS));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_UUID_IDS, supported = false)
        public void shouldSupportUuidIdsIfUuidIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            final VertexProperty p = v.property(VertexProperty.Cardinality.single, "name", "stephen");
            if (p.id() instanceof UUID)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), VertexPropertyFeatures.FEATURE_UUID_IDS));
        }

        @Test
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexPropertyFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportNumericIdsIfNumericIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = graph.addVertex();
            final VertexProperty p = v.property(VertexProperty.Cardinality.single, "name", "stephen");
            if (p.id() instanceof Number)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), VertexPropertyFeatures.FEATURE_NUMERIC_IDS));
        }


        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = VertexPropertyFeatures.FEATURE_REMOVE_PROPERTY, supported = false)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = VertexFeatures.FEATURE_META_PROPERTIES)
        @FeatureRequirementSet(FeatureRequirementSet.Package.VERTICES_ONLY)
        public void shouldSupportRemovePropertyIfAPropertyCanBeRemoved() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                final VertexProperty p = v.property(VertexProperty.Cardinality.single, "name", "me", "test", "this");
                p.property("test").remove();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), VertexPropertyFeatures.FEATURE_REMOVE_PROPERTY));
            } catch (Exception ex) {
                validateException(Property.Exceptions.propertyRemovalNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = VertexFeatures.FEATURE_MULTI_PROPERTIES, supported = false)
        public void shouldSupportMultiPropertyIfTheSameKeyCanBeAssignedMoreThanOnce() throws Exception {
            try {
                final Vertex v = graph.addVertex("name", "stephen", "name", "steve");
                if (2 == IteratorUtils.count(v.properties()))
                    fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexFeatures.FEATURE_MULTI_PROPERTIES));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.multiPropertiesNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = VertexFeatures.FEATURE_META_PROPERTIES, supported = false)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = VertexPropertyFeatures.FEATURE_ADD_PROPERTY)
        public void shouldSupportMetaPropertyIfPropertiesCanBePutOnProperties() throws Exception {
            try {
                final Vertex v = graph.addVertex();
                v.property(VertexProperty.Cardinality.single, "name", "stephen", "p", "on-property");
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexFeatures.FEATURE_META_PROPERTIES));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.metaPropertiesNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = VertexFeatures.FEATURE_META_PROPERTIES, supported = false)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = VertexPropertyFeatures.FEATURE_ADD_PROPERTY)
        public void shouldSupportMetaPropertyIfPropertiesCanBePutOnPropertiesViaVertexProperty() throws Exception {
            try {
                final Vertex v = graph.addVertex("name", "stephen");
                v.property("name").property("p", "on-property");
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexFeatures.FEATURE_META_PROPERTIES));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.metaPropertiesNotSupported(), ex);
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = VertexFeatures.FEATURE_META_PROPERTIES, supported = false)
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = VertexPropertyFeatures.FEATURE_ADD_PROPERTY)
        public void shouldSupportMetaPropertyIfPropertiesHaveAnIteratorViaVertexProperty() throws Exception {
            try {
                final Vertex v = graph.addVertex("name", "stephen");
                v.property("name").properties();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), VertexFeatures.FEATURE_META_PROPERTIES));
            } catch (Exception ex) {
                validateException(VertexProperty.Exceptions.metaPropertiesNotSupported(), ex);
            }
        }
    }

    /**
     * Feature checks that simply evaluate conflicting feature definitions without evaluating the actual implementation
     * itself.
     */
    public static class LogicalFeatureSupportTest extends AbstractGremlinTest {

        private EdgeFeatures edgeFeatures;
        private EdgePropertyFeatures edgePropertyFeatures;
        private GraphFeatures graphFeatures;
        private Graph.Features.VariableFeatures variablesFeatures;
        private VertexFeatures vertexFeatures;
        private VertexPropertyFeatures vertexPropertyFeatures;

        @Before
        public void innerSetup() {
            final Graph.Features f = graph.features();
            edgeFeatures = f.edge();
            edgePropertyFeatures = edgeFeatures.properties();
            graphFeatures = f.graph();
            variablesFeatures = graphFeatures.variables();
            vertexFeatures = f.vertex();
            vertexPropertyFeatures = vertexFeatures.properties();
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = VertexFeatures.FEATURE_ANY_IDS)
        public void shouldNotSupportAnyIdsUnlessUserSuppliedIdsIsSupportedOnVertex() {
            assertTrue(vertexFeatures.supportsUserSuppliedIds());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = EdgeFeatures.FEATURE_ANY_IDS)
        public void shouldNotSupportAnyIdsUnlessUserSuppliedIdsIsSupportedOnEdge() {
            assertTrue(edgeFeatures.supportsUserSuppliedIds());
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = VertexPropertyFeatures.FEATURE_ANY_IDS)
        public void shouldNotSupportAnyIdsUnlessUserSuppliedIdsIsSupportedOnVertexProperty() {
            assertTrue(vertexPropertyFeatures.supportsUserSuppliedIds());
        }

        @Test
        public void shouldSupportADataTypeIfGraphHasVariablesEnabled() {
            assertEquals(variablesFeatures.supportsVariables(), (variablesFeatures.supportsBooleanValues() || variablesFeatures.supportsDoubleValues()
                    || variablesFeatures.supportsFloatValues() || variablesFeatures.supportsIntegerValues()
                    || variablesFeatures.supportsLongValues() || variablesFeatures.supportsMapValues()
                    || variablesFeatures.supportsMixedListValues() || variablesFeatures.supportsByteValues()
                    || variablesFeatures.supportsBooleanArrayValues() || variablesFeatures.supportsByteArrayValues()
                    || variablesFeatures.supportsDoubleArrayValues() || variablesFeatures.supportsFloatArrayValues()
                    || variablesFeatures.supportsIntegerArrayValues() || variablesFeatures.supportsLongArrayValues()
                    || variablesFeatures.supportsSerializableValues() || variablesFeatures.supportsStringValues()
                    || variablesFeatures.supportsUniformListValues() || variablesFeatures.supportsStringArrayValues()));
        }

        @Test
        public void shouldSupportADataTypeIfEdgeHasPropertyEnabled() {
            assertEquals(edgePropertyFeatures.supportsProperties(), (edgePropertyFeatures.supportsBooleanValues() || edgePropertyFeatures.supportsDoubleValues()
                    || edgePropertyFeatures.supportsFloatValues() || edgePropertyFeatures.supportsIntegerValues()
                    || edgePropertyFeatures.supportsLongValues() || edgePropertyFeatures.supportsMapValues()
                    || edgePropertyFeatures.supportsMixedListValues() || edgePropertyFeatures.supportsByteValues()
                    || edgePropertyFeatures.supportsBooleanArrayValues() || edgePropertyFeatures.supportsByteArrayValues()
                    || edgePropertyFeatures.supportsDoubleArrayValues() || edgePropertyFeatures.supportsFloatArrayValues()
                    || edgePropertyFeatures.supportsIntegerArrayValues() || edgePropertyFeatures.supportsLongArrayValues()
                    || edgePropertyFeatures.supportsSerializableValues() || edgePropertyFeatures.supportsStringValues()
                    || edgePropertyFeatures.supportsUniformListValues() || edgePropertyFeatures.supportsStringArrayValues()));
        }

        @Test
        public void shouldSupportADataTypeIfVertexHasPropertyEnabled() {
            assertEquals(vertexPropertyFeatures.supportsProperties(), (vertexPropertyFeatures.supportsBooleanValues() || vertexPropertyFeatures.supportsDoubleValues()
                    || vertexPropertyFeatures.supportsFloatValues() || vertexPropertyFeatures.supportsIntegerValues()
                    || vertexPropertyFeatures.supportsLongValues() || vertexPropertyFeatures.supportsMapValues()
                    || vertexPropertyFeatures.supportsMixedListValues() || vertexPropertyFeatures.supportsByteValues()
                    || vertexPropertyFeatures.supportsBooleanArrayValues() || vertexPropertyFeatures.supportsByteArrayValues()
                    || vertexPropertyFeatures.supportsDoubleArrayValues() || vertexPropertyFeatures.supportsFloatArrayValues()
                    || vertexPropertyFeatures.supportsIntegerArrayValues() || vertexPropertyFeatures.supportsLongArrayValues()
                    || vertexPropertyFeatures.supportsSerializableValues() || vertexPropertyFeatures.supportsStringValues()
                    || vertexPropertyFeatures.supportsUniformListValues() || vertexPropertyFeatures.supportsStringArrayValues()));
        }

        @Test
        public void shouldSupportRegularTransactionsIfThreadedTransactionsAreEnabled() {
            if (graphFeatures.supportsThreadedTransactions())
                assertTrue(graphFeatures.supportsThreadedTransactions());
        }
    }
}
