package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.Graph.Features.GraphFeatures;
import com.tinkerpop.blueprints.Graph.Features.VertexFeatures;
import org.junit.Test;

import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_COMPUTER;
import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.blueprints.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

/**
 * Tests that do basic validation of proper Feature settings in Graph implementations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FeatureSupportTest extends AbstractBlueprintsTest {
    private static final String INVALID_FEATURE_SPECIFICATION = "Features specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_COMPUTER, supported = false)
    public void shouldEnableGraphFeatureSupportsComputer() throws Exception {
        try {
            g.compute();
            fail(String.format(INVALID_FEATURE_SPECIFICATION, FEATURE_COMPUTER));
        } catch (UnsupportedOperationException e) {
            assertEquals(Graph.Exceptions.graphComputerNotSupported().getMessage(), e.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = GraphFeatures.class, feature = FEATURE_TRANSACTIONS, supported = false)
    public void shouldEnableGraphFeatureSupportsTransactions() throws Exception {
        try {
            g.tx();
            fail(String.format(INVALID_FEATURE_SPECIFICATION, FEATURE_TRANSACTIONS));
        } catch (UnsupportedOperationException e) {
            assertEquals(Graph.Exceptions.transactionsNotSupported().getMessage(), e.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
    public void shouldEnableVertexFeatureUserSuppliedIds() throws Exception {
        final Vertex v = g.addVertex(Property.Key.ID, BlueprintsSuite.GraphManager.get().convertId(99999943835l));

        // can't define this feature as a @FeatureRequirement because the test should run regardless of the
        // transactional capability of the graph. need to potentially commit here because some graphs won't
        // assign an permanent ID until after commit.
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().commit();

        assertThat(String.format(INVALID_FEATURE_SPECIFICATION, FEATURE_USER_SUPPLIED_IDS),
                v.getId().toString(),
                is(not("99999943835")));
    }
}
