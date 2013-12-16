package com.tinkerpop.blueprints;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeThat;

/**
 * Sets up g based on the current graph configuration and checks required features for the test.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractBlueprintsTest {
    protected Graph g;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setup() throws Exception {
        g = BlueprintsSuite.GraphManager.get().newTestGraph();

        final Method testMethod = this.getClass().getMethod(name.getMethodName());
        final FeatureRequirement[] featureRequirement = testMethod.getAnnotationsByType(FeatureRequirement.class);
        final List<FeatureRequirement> frs = Arrays.asList(featureRequirement);
        for (FeatureRequirement fr : frs) {
            assumeThat(g.getFeatures().supports(fr.featureClass(), fr.feature()), is(fr.supported()));
        }
    }
}
