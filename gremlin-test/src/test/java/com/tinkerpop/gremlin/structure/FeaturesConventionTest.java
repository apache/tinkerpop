package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * All features should be prefixed with the word "supports" and return boolean.  Furthermore, all should have an
 * FeatureDescriptor annotation with a "name" that represents the suffix after "supports" in the same case as the
 * method.  A String name of the feature should be supplied as a public static variable and be equal to the value
 * of the "name" in all upper case.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class FeaturesConventionTest {
    private static final String FEATURE_METHOD_PREFIX = "supports";
    private static final String FEATURE_FIELD_PREFIX = "FEATURE_";

    private static final String ERROR_FIELD = "Feature [%s] must have a field declared with the name of the feature as 'public static final'";

    @Parameterized.Parameters(name = "{0}.test()")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {Graph.Features.EdgeFeatures.class},
                {Graph.Features.EdgePropertyFeatures.class},
                {Graph.Features.GraphFeatures.class},
                {Graph.Features.VariableFeatures.class},
                {Graph.Features.PropertyFeatures.class},
                {Graph.Features.VertexFeatures.class}
        });
    }

    @Parameterized.Parameter(value = 0)
    public Class<?> featuresClass;

    @Test
    public void shouldFollowConventionsForFeatures() {
        Arrays.asList(featuresClass.getMethods()).stream()
                .filter(FeaturesConventionTest::chooseFeatureMethod)
                .forEach(FeaturesConventionTest::assertFeatureConvention);
    }

    private static String convertToUnderscore(final String text) {
        final String regex = "([a-z])([A-Z])";
        final String replacement = "$1_$2";
        final String underscored = text.replaceAll(regex, replacement);
        return underscored.substring(0, underscored.length());
    }

    private static boolean chooseFeatureMethod(Method m) {
        return m.getName().startsWith(FEATURE_METHOD_PREFIX) && !m.getName().equals(FEATURE_METHOD_PREFIX);
    }

    private static void assertFeatureConvention(final Method m) {
        final FeatureDescriptor annotation = m.getAnnotation(FeatureDescriptor.class);
        final Class featuresClass = m.getDeclaringClass();

        // all "features" need an annotation
        assertNotNull(annotation);

        // needs to match pattern of "support" followed by "name" value in annotation
        assertEquals(m.getName(), FEATURE_METHOD_PREFIX + annotation.name());

        try {
            final Field f = featuresClass.getField(FEATURE_FIELD_PREFIX + convertToUnderscore(annotation.name()).toUpperCase());
            assertEquals(annotation.name(), f.get(null));
        } catch (Exception e) {
            fail(String.format(ERROR_FIELD, annotation.name()));
        }
    }
}
