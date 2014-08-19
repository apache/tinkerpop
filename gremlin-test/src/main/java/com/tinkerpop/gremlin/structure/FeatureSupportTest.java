package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.ExceptionCoverage;
import com.tinkerpop.gremlin.FeatureRequirement;
import com.tinkerpop.gremlin.GraphManager;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgeFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import com.tinkerpop.gremlin.structure.Graph.Features.VertexPropertyFeatures;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.UUID;

import static com.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_STRING_IDS;
import static com.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_NUMERIC_IDS;
import static com.tinkerpop.gremlin.structure.Graph.Features.ElementFeatures.FEATURE_UUID_IDS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_COMPUTER;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_THREADED_TRANSACTIONS;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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

    public static class FeatureToStringTest extends AbstractGremlinTest {
        /**
         * A rough test to ensure that StringFactory is being used to toString Features.
         */
        @Test
        public void shouldHaveStandardToStringRepresentation() {
            assertTrue(g.getFeatures().toString().startsWith("FEATURES"));
        }
    }

    /**
     * Feature checks that test {@link com.tinkerpop.gremlin.structure.Graph} functionality to determine if a feature should be on when it is marked
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
            System.out.println(String.format("Printing Features of %s for reference: ", g.getClass().getSimpleName()));
            System.out.println(g.getFeatures());
            assertTrue(true);
        }

        /**
         * A {@link com.tinkerpop.gremlin.structure.Graph} that does not support {@link com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_COMPUTER} must call
         * {@link com.tinkerpop.gremlin.structure.Graph.Exceptions#graphComputerNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_COMPUTER, supported = false)
        public void shouldSupportComputerIfAGraphCanCompute() throws Exception {
            try {
                g.compute();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_COMPUTER));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.graphComputerNotSupported().getMessage(), e.getMessage());
            }
        }

        /**
         * A {@link com.tinkerpop.gremlin.structure.Graph} that does not support {@link com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures#FEATURE_TRANSACTIONS} must call
         * {@link com.tinkerpop.gremlin.structure.Graph.Exceptions#transactionsNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_TRANSACTIONS, supported = false)
        public void shouldSupportTransactionsIfAGraphConstructsATx() throws Exception {
            try {
                g.tx();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_TRANSACTIONS));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.transactionsNotSupported().getMessage(), e.getMessage());
            }
        }

        /**
         * A {@link com.tinkerpop.gremlin.structure.Graph} that does not support {@link com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures#FEATURE_VARIABLES} must call
         * {@link com.tinkerpop.gremlin.structure.Graph.Exceptions#variablesNotSupported()}.
         */
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES, supported = false)
        public void shouldSupportMemoryIfAGraphAcceptsMemory() throws Exception {
            try {
                g.variables();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, Graph.Features.VariableFeatures.class.getSimpleName(), FEATURE_VARIABLES));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.variablesNotSupported().getMessage(), e.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_THREADED_TRANSACTIONS, supported = false)
        public void shouldThrowOnThreadedTransactionNotSupported() {
            try {
                g.tx().create();
                fail("An exception should be thrown since the threaded transaction feature is not supported");
            } catch (Exception ex) {
                final Exception expectedException = Transaction.Exceptions.threadedTransactionsNotSupported();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Feature checks that test {@link com.tinkerpop.gremlin.structure.Vertex} functionality to determine if a feature
     * should be on when it is marked as not supported.
     */
    public static class VertexFunctionalityTest extends AbstractGremlinTest {

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsIfAnIdCanBeAssignedToVertex() throws Exception {
            try {
                g.addVertex(Element.ID, GraphManager.get().convertId(99999943835l));
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_USER_SUPPLIED_IDS));
            } catch (Exception ex) {
                assertEquals(Vertex.Exceptions.userSuppliedIdsNotSupported().getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_STRING_IDS, supported = false)
        public void shouldSupportStringIdsIfStringIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = g.addVertex();
            if (v.id() instanceof String)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_STRING_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_UUID_IDS, supported = false)
        public void shouldSupportStringIdsIfUuidIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = g.addVertex();
            if (v.id() instanceof UUID)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_UUID_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportStringIdsIfNumericIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = g.addVertex();
            if (v.id() instanceof Number)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
        }
    }

    /**
     * Feature checks that test {@link com.tinkerpop.gremlin.structure.Edge} functionality to determine if a feature
     * should be on when it is marked as not supported.
     */
    public static class EdgeFunctionalityTest extends AbstractGremlinTest {

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = EdgeFeatures.FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void shouldSupportUserSuppliedIdsIfAnIdCanBeAssignedToEdge() throws Exception {
            try {
                final Vertex v = g.addVertex();
                v.addEdge("friend", v, Element.ID, GraphManager.get().convertId(99999943835l));
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), EdgeFeatures.FEATURE_USER_SUPPLIED_IDS));
            } catch (Exception ex) {
                assertEquals(Edge.Exceptions.userSuppliedIdsNotSupported().getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_STRING_IDS, supported = false)
        public void shouldSupportStringIdsIfStringIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = g.addVertex();
            final Edge e = v.addEdge("knows", v);
            if (e.id() instanceof String)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_STRING_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_UUID_IDS, supported = false)
        public void shouldSupportStringIdsIfUuidIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = g.addVertex();
            final Edge e = v.addEdge("knows", v);
            if (e.id() instanceof UUID)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_UUID_IDS));
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_ADD_EDGES)
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        @FeatureRequirement(featureClass = EdgeFeatures.class, feature = FEATURE_NUMERIC_IDS, supported = false)
        public void shouldSupportStringIdsIfNumericIdsAreGeneratedFromTheGraph() throws Exception {
            final Vertex v = g.addVertex();
            final Edge e = v.addEdge("knows", v);
            if (e.id() instanceof Number)
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgeFeatures.class.getSimpleName(), FEATURE_NUMERIC_IDS));
        }
    }

    /**
     * Feature checks that test {@link com.tinkerpop.gremlin.structure.Element} {@link com.tinkerpop.gremlin.structure.Property} functionality to determine if a feature should be on
     * when it is marked as not supported.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "dataTypeOfPropertyValueNotSupported"
    })
    public static class ElementPropertyFunctionalityTest extends AbstractGremlinTest {
        private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

        @Parameterized.Parameters(name = "{index}: supports{0}({1})")
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
        public void shouldEnableFeatureOnEdgeIfNotEnabled() throws Exception {
            assumeThat(g.getFeatures().supports(EdgePropertyFeatures.class, featureName), is(false));
            try {
                final Edge edge = createEdgeForPropertyFeatureTests();
                edge.property("key", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, EdgePropertyFeatures.class.getSimpleName(), featureName));
            } catch (UnsupportedOperationException e) {
                assertEquals(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value).getMessage(), e.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_ADD_VERTICES)
        public void shouldEnableFeatureOnVertexIfNotEnabled() throws Exception {
            assumeThat(g.getFeatures().supports(VertexPropertyFeatures.class, featureName), is(false));
            try {
                g.addVertex("key", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, VertexPropertyFeatures.class.getSimpleName(), featureName));
            } catch (UnsupportedOperationException e) {
                assertEquals(Property.Exceptions.dataTypeOfPropertyValueNotSupported(value).getMessage(), e.getMessage());
            }
        }

        private Edge createEdgeForPropertyFeatureTests() {
            final Vertex vertexA = g.addVertex();
            final Vertex vertexB = g.addVertex();
            return vertexA.addEdge(GraphManager.get().convertLabel("knows"), vertexB);
        }
    }

    /**
     * Feature checks that tests if {@link com.tinkerpop.gremlin.structure.Graph.Variables}
     * functionality to determine if a feature should be on when it is marked as not supported.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Graph.Variables.Exceptions.class, methods = {
            "dataTypeOfVariableValueNotSupported"
    })
    public static class MemoryFunctionalityTest extends AbstractGremlinTest {
        private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

        @Parameterized.Parameters(name = "{index}: supports{0}({1})")
        public static Iterable<Object[]> data() {
            return VariablesTest.MemoryFeatureSupportTest.data();
        }

        @Parameterized.Parameter(value = 0)
        public String featureName;

        @Parameterized.Parameter(value = 1)
        public Object value;

        /**
         * In this case, the feature requirement for memory is checked, because it means that at least one aspect of
         * memory is supported so we need to test other features to make sure they work properly.
         */
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = Graph.Features.VariableFeatures.FEATURE_VARIABLES)
        public void shouldEnableFeatureOnGraphIfNotEnabled() throws Exception {
            assumeThat(g.getFeatures().supports(Graph.Features.VariableFeatures.class, featureName), is(false));
            try {
                final Graph.Variables variables = g.variables();
                variables.set("key", value);
                fail(String.format(INVALID_FEATURE_SPECIFICATION, Graph.Features.VariableFeatures.class.getSimpleName(), featureName));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(value).getMessage(), e.getMessage());
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
        private Graph.Features.VariableFeatures variableFeatures;
        private VertexFeatures vertexFeatures;
        private VertexPropertyFeatures vertexPropertyFeatures;

        @Before
        public void innerSetup() {
            final Graph.Features f = g.getFeatures();
            edgeFeatures = f.edge();
            edgePropertyFeatures = edgeFeatures.properties();
            graphFeatures = f.graph();
            variableFeatures = graphFeatures.variables();
            vertexFeatures = f.vertex();
            vertexPropertyFeatures = vertexFeatures.properties();
        }

        @Test
        public void shouldSupportADataTypeIfGraphHasMemoryEnabled() {
            assertEquals(variableFeatures.supportsVariables(), (variableFeatures.supportsBooleanValues() || variableFeatures.supportsDoubleValues()
                    || variableFeatures.supportsFloatValues() || variableFeatures.supportsIntegerValues()
                    || variableFeatures.supportsLongValues() || variableFeatures.supportsMapValues()
                    || variableFeatures.supportsMixedListValues()|| variableFeatures.supportsByteValues()
                    || variableFeatures.supportsBooleanArrayValues() || variableFeatures.supportsByteArrayValues()
                    || variableFeatures.supportsDoubleArrayValues() || variableFeatures.supportsFloatArrayValues()
                    || variableFeatures.supportsIntegerArrayValues() || variableFeatures.supportsLongArrayValues()
                    || variableFeatures.supportsSerializableValues() || variableFeatures.supportsStringValues()
                    || variableFeatures.supportsUniformListValues() || variableFeatures.supportsStringArrayValues()));
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
