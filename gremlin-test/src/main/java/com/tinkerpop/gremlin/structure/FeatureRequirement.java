package com.tinkerpop.gremlin.structure;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A FeatureRequirement annotation defines a feature to check for a test in the Blueprints Test Suite.  The
 * annotation needs to be added to each test that requires a check for feature support.  Multiple memory may
 * be added for each feature to check.
 * <p/>
 * Tests should not directly test for features using the FeatureSet classes/methods with if/then type statements in
 * the tests themselves as the logic for whether the test gets executed is lost in the code and auto-passes tests
 * when the check for the feature has a negative result.  Extracting such logic for feature support for a test into
 * the FeatureRequirement annotation will "ignore" a test rather than pass it, making it easier for implementers to
 * see exactly which tests are being evaluated during test time.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Repeatable(FeatureRequirements.class)
public @interface FeatureRequirement {
    /**
     * The name of the feature as defined by the <code>public static final</code> member variable on each FeatureSet
     * implementation.
     */
    String feature();

    /**
     * The FeatureSet extension interface that owns the feature to be tested.
     */
    Class<? extends Graph.Features.FeatureSet> featureClass();

    /**
     * Denotes if the test should be executed if the feature is supported or unsupported.  By default this value is
     * set to true.
     */
    boolean supported() default true;
}
