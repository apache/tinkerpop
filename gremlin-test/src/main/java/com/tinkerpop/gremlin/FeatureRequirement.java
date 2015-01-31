package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.structure.Graph;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A FeatureRequirement annotation defines a feature to check for a test in the Gremlin  Test Suite.  The
 * annotation needs to be added to each test that requires a check for feature support.  Multiple sideEffects may
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

    public static class Factory {
        public static FeatureRequirement create(final String feature, final Class<? extends Graph.Features.FeatureSet> featureClass) {
            return create(feature, featureClass, true);
        }

        public static FeatureRequirement create(final String feature, final Class<? extends Graph.Features.FeatureSet> featureClass,
                                                final boolean supported) {
            return new FeatureRequirement() {
                @Override
                public String feature() {
                    return feature;
                }

                @Override
                public Class<? extends Graph.Features.FeatureSet> featureClass() {
                    return featureClass;
                }

                @Override
                public boolean supported() {
                    return supported;
                }

                @Override
                public Class<? extends Annotation> annotationType() {
                    return FeatureRequirement.class;
                }
            };
        }
    }
}
