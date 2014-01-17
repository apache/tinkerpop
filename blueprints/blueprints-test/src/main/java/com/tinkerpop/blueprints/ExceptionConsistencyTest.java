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

/**
 * Ensure that exception handling is consistent within Blueprints.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
public class ExceptionConsistencyTest {

    /**
     * Checks that properties added to an {@link Element} are validated in a consistent way when they are added at
     * construction by throwing an appropriate exception.
     */
    @RunWith(Parameterized.class)
    public static class PropertyValidationOnAddTest extends AbstractBlueprintsTest {

        @Parameterized.Parameters(name = "{index}: testGraphAddVertex({2})")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    { Graph.Features.PropertyFeatures.FEATURE_PROPERTIES,
                      new Object[] {"odd", "number", "arguments"},
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
        public Object[] arguments;

        @Parameterized.Parameter(value = 2)
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
}
