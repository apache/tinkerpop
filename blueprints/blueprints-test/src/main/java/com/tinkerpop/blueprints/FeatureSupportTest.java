package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.Graph.Features.EdgeFeatures;
import com.tinkerpop.blueprints.Graph.Features.EdgePropertyFeatures;
import com.tinkerpop.blueprints.Graph.Features.GraphFeatures;
import com.tinkerpop.blueprints.Graph.Features.GraphPropertyFeatures;
import com.tinkerpop.blueprints.Graph.Features.VertexFeatures;
import com.tinkerpop.blueprints.Graph.Features.VertexPropertyFeatures;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_COMPUTER;
import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.blueprints.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

/**
 * Tests that do basic validation of proper Feature settings in Graph implementations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class FeatureSupportTest  {
    private static final String INVALID_FEATURE_SPECIFICATION = "Features for %s specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

    /**
     * Feature checks that test functionality to determine if a feature should be on or off.
     */
    public static class FunctionalityTest extends AbstractBlueprintsTest {
        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_COMPUTER, supported = false)
        public void ifAGraphCanComputeThenItMustSupportComputer() throws Exception {
            try {
                g.compute();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_COMPUTER));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.graphComputerNotSupported().getMessage(), e.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_TRANSACTIONS, supported = false)
        public void ifAGraphConstructsATxThenItMustSupportTransactions() throws Exception {
            try {
                g.tx();
                fail(String.format(INVALID_FEATURE_SPECIFICATION, GraphFeatures.class.getSimpleName(), FEATURE_TRANSACTIONS));
            } catch (UnsupportedOperationException e) {
                assertEquals(Graph.Exceptions.transactionsNotSupported().getMessage(), e.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void ifAnIdCanBeAssignedToVertexThenItMustSupportUserSuppliedIds() throws Exception {
            final Vertex v = g.addVertex(Property.Key.ID, BlueprintsSuite.GraphManager.get().convertId(99999943835l));

            // can't define this feature as a @FeatureRequirement because the test should run regardless of the
            // transactional capability of the graph. need to potentially commit here because some graphs won't
            // assign an permanent ID until after commit.
            if (g.getFeatures().graph().supportsTransactions())
                g.tx().commit();

            assertThat(String.format(INVALID_FEATURE_SPECIFICATION, VertexFeatures.class.getSimpleName(), FEATURE_USER_SUPPLIED_IDS),
                    v.getId().toString(),
                    is(not("99999943835")));
        }
    }

    /**
     * Feature checks that simply evaluate conflicting feature definitions without evaluating the actual implementation
     * itself.
     */
    public static class LogicalFeatureSupportTest extends AbstractBlueprintsTest {

        private EdgeFeatures edgeFeatures;
        private EdgePropertyFeatures edgePropertyFeatures;
        private GraphFeatures graphFeatures;
        private GraphPropertyFeatures graphPropertyFeatures;
        private VertexFeatures vertexFeatures;
        private VertexPropertyFeatures vertexPropertyFeatures;

        @Before
        public void innerSetup() {
            final Graph.Features f = g.getFeatures();
            edgeFeatures = f.edge();
            edgePropertyFeatures = edgeFeatures.properties();
            graphFeatures = f.graph();
            graphPropertyFeatures = graphFeatures.properties();
            vertexFeatures = f.vertex();
            vertexPropertyFeatures = vertexFeatures.properties();
        }

        @Test
        public void ifGraphHasPropertyEnabledThenItMustSupportADataType() {
            assertTrue(graphPropertyFeatures.supportsProperties()
                    && (graphPropertyFeatures.supportsBooleanValues() || graphPropertyFeatures.supportsDoubleValues()
                    || graphPropertyFeatures.supportsFloatValues() || graphPropertyFeatures.supportsIntegerValues()
                    || graphPropertyFeatures.supportsLongValues() || graphPropertyFeatures.supportsMapValues()
                    || graphPropertyFeatures.supportsMetaProperties() || graphPropertyFeatures.supportsMixedListValues()
                    || graphPropertyFeatures.supportsPrimitiveArrayValues() || graphPropertyFeatures.supportsPrimitiveArrayValues()
                    || graphPropertyFeatures.supportsSerializableValues() || graphPropertyFeatures.supportsStringValues()
                    || graphPropertyFeatures.supportsUniformListValues()));
        }

        @Test
        public void ifEdgeHasPropertyEnabledThenItMustSupportADataType() {
            assertTrue(edgePropertyFeatures.supportsProperties()
                    && (edgePropertyFeatures.supportsBooleanValues() || edgePropertyFeatures.supportsDoubleValues()
                    || edgePropertyFeatures.supportsFloatValues() || edgePropertyFeatures.supportsIntegerValues()
                    || edgePropertyFeatures.supportsLongValues() || edgePropertyFeatures.supportsMapValues()
                    || edgePropertyFeatures.supportsMetaProperties() || edgePropertyFeatures.supportsMixedListValues()
                    || edgePropertyFeatures.supportsPrimitiveArrayValues() || edgePropertyFeatures.supportsPrimitiveArrayValues()
                    || edgePropertyFeatures.supportsSerializableValues() || edgePropertyFeatures.supportsStringValues()
                    || edgePropertyFeatures.supportsUniformListValues()));
        }

        @Test
        public void ifVertexHasPropertyEnabledThenItMustSupportADataType() {
            assertTrue(vertexPropertyFeatures.supportsProperties()
            && (vertexPropertyFeatures.supportsBooleanValues() || vertexPropertyFeatures.supportsDoubleValues()
                    || vertexPropertyFeatures.supportsFloatValues() || vertexPropertyFeatures.supportsIntegerValues()
                    || vertexPropertyFeatures.supportsLongValues() || vertexPropertyFeatures.supportsMapValues()
                    || vertexPropertyFeatures.supportsMetaProperties() || vertexPropertyFeatures.supportsMixedListValues()
                    || vertexPropertyFeatures.supportsPrimitiveArrayValues() || vertexPropertyFeatures.supportsPrimitiveArrayValues()
                    || vertexPropertyFeatures.supportsSerializableValues() || vertexPropertyFeatures.supportsStringValues()
                    || vertexPropertyFeatures.supportsUniformListValues()));
        }


    }
}
