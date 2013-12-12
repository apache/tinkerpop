package com.tinkerpop.blueprints;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests that do basic validation of proper Feature settings in Graph implementations.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FeatureSupportTest extends AbstractBlueprintsTest {
    private static final String INVALID_FEATURE_SPECIFICATION = "Features specify that %s is false, but the feature appears to be implemented.  Reconsider this setting or throw the standard Exception.";

    @Test
    @FeatureRequirement(featureClass = Graph.Features.class, feature = Graph.Features.FEATURE_COMPUTER, supported = false)
    public void shouldEnableGraphFeatureSupportsComputer() throws Exception {
        try {
            g.compute();
            fail(String.format(INVALID_FEATURE_SPECIFICATION, Graph.Features.FEATURE_COMPUTER));
        } catch (UnsupportedOperationException e) {
            assertEquals(Graph.Exceptions.graphComputerNotSupported().getMessage(), e.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.class, feature = Graph.Features.FEATURE_TRANSACTIONS, supported = false)
    public void shouldEnableGraphFeatureSupportsTransactions() throws Exception {
        try {
            g.tx();
            fail(String.format(INVALID_FEATURE_SPECIFICATION, Graph.Features.FEATURE_TRANSACTIONS));
        } catch (UnsupportedOperationException e) {
            assertEquals(Graph.Exceptions.transactionsNotSupported().getMessage(), e.getMessage());
        }
    }
}
