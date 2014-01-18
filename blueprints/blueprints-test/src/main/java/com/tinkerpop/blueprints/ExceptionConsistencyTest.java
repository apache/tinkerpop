package com.tinkerpop.blueprints;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;

/**
 * Ensure that exception handling is consistent within Blueprints.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class ExceptionConsistencyTest {

    /**
     * Checks that properties added to an {@link Element} are validated in a consistent way when they are added at
     * {@link Vertex} or {@link Edge} construction by throwing an appropriate exception.
     */
    @RunWith(Parameterized.class)
    public static class PropertyValidationOnAddTest extends AbstractBlueprintsTest {

        @Parameterized.Parameters(name = "{index}: add({1})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    { new Object[] {"odd", "number", "arguments"},Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    { new Object[] {"odd"}, Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    { new Object[] {"odd", "number", 123, "test"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    { new Object[] {"odd", null}, Property.Exceptions.propertyValueCanNotBeNull()},
                    { new Object[] {null, "val"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    { new Object[] {"", "val"}, Property.Exceptions.propertyKeyCanNotBeEmpty()}});
            };

        @Parameterized.Parameter(value = 0)
        public Object[] arguments;

        @Parameterized.Parameter(value = 1)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void testGraphAddVertex() throws Exception {
            try {
                this.g.addVertex(arguments);
                fail(String.format("Call to addVertex should have thrown an exception with these arguments [%s]", arguments));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void testGraphAddEdge() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v, arguments);
                fail(String.format("Call to addVertex should have thrown an exception with these arguments [%s]", arguments));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Checks that properties added to an {@link Element} are validated in a consistent way when they are set after
     * {@link Vertex} or {@link Edge} construction by throwing an appropriate exception.
     */
    /*
    @RunWith(Parameterized.class)
    public static class PropertyValidationOnSetTest extends AbstractBlueprintsTest {

        @Parameterized.Parameters(name = "{index}: validate({2})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    { null, "val",
                      Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    { Graph.Features.PropertyFeatures.FEATURE_PROPERTIES,
                            new Object[] {"odd"},
                            Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    { Graph.Features.PropertyFeatures.FEATURE_PROPERTIES,
                            new Object[] {"odd", "number", 123, "test"},
                            Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    { Graph.Features.PropertyFeatures.FEATURE_PROPERTIES,
                            new Object[] {"odd", null},
                            Property.Exceptions.propertyValueCanNotBeNull()},
                    { Graph.Features.PropertyFeatures.FEATURE_PROPERTIES,
                            new Object[] {null, "val"},
                            Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    { Graph.Features.PropertyFeatures.FEATURE_PROPERTIES,
                            new Object[] {"", "val"},
                            Property.Exceptions.propertyKeyCanNotBeEmpty()}});
        };

        @Parameterized.Parameter(value = 0)
        public String featureName;

        @Parameterized.Parameter(value = 1)
        public String key;

        @Parameterized.Parameter(value = 2)
        public Object val;

        @Parameterized.Parameter(value = 3)
        public Exception expectedException;

        @Test
        public void testGraphAddVertex() throws Exception {
            assumeThat(g.getFeatures().supports(Graph.Features.VertexPropertyFeatures.class, featureName), is(true));

            try {
                this.g.addVertex(arguments);
                fail(String.format("Call to addVertex should have thrown an exception with these arguments [%s]", arguments));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        public void testGraphAddEdge() throws Exception {
            assumeThat(g.getFeatures().supports(Graph.Features.EdgePropertyFeatures.class, featureName), is(true));

            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v, arguments);
                fail(String.format("Call to addVertex should have thrown an exception with these arguments [%s]", arguments));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }
    */
}
