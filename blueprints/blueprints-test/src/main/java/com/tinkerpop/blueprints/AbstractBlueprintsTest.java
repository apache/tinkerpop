package com.tinkerpop.blueprints;

import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Sets up g based on the current graph configuration and checks required features for the test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractBlueprintsTest {
    protected Graph g;
    protected Configuration config;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() throws Exception {
        config = BlueprintsSuite.GraphManager.get().newGraphConfiguration();
        g = BlueprintsSuite.GraphManager.get().newTestGraph(config);

        final Method testMethod = this.getClass().getMethod(name.getMethodName());
        final FeatureRequirement[] featureRequirement = testMethod.getAnnotationsByType(FeatureRequirement.class);
        final List<FeatureRequirement> frs = Arrays.asList(featureRequirement);
        for (FeatureRequirement fr : frs) {
            assumeThat(g.getFeatures().supports(fr.featureClass(), fr.feature()), is(fr.supported()));
        }
    }

    @After
    public void tearDown() throws Exception {
        BlueprintsSuite.GraphManager.get().clear(g, config);
    }

    /**
     * Utility method that commits if the graph supports transactions.
     */
    protected void tryCommit(final Graph g) {
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().commit();
    }

    /**
     * Utility method that commits if the graph supports transactions and executes an assertion function before and
     * after the commit.  It assumes that the assertion should be true before and after the commit.
     */
    protected void tryCommit(final Graph g, final Consumer<Graph> assertFunction) {
        assertFunction.accept(g);
        if (g.getFeatures().graph().supportsTransactions()) {
            g.tx().commit();
            assertFunction.accept(g);
        }
    }

    /**
     * Utility method that rollsback if the graph supports transactions.
     */
    protected void tryRollback(final Graph g) {
        if (g.getFeatures().graph().supportsTransactions())
            g.tx().rollback();
    }
}
