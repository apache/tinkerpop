package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.computer.VertexProgram;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_COMPUTER;

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

        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Parameterized.Parameters(name = "{index}: expect - {1}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    { new Object[] {"odd", "number", "arguments"},Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    { new Object[] {"odd"}, Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    { new Object[] {"odd", "number", 123, "test"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    { new Object[] {"odd", null}, Property.Exceptions.propertyValueCanNotBeNull()},
                    { new Object[] {null, "val"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    { new Object[] {"", "val"}, Property.Exceptions.propertyKeyCanNotBeEmpty()}});
            }

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
    @RunWith(Parameterized.class)
    public static class PropertyValidationOnSetTest extends AbstractBlueprintsTest {

        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Parameterized.Parameters(name = "{index}: expect - {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    { "k", null, Property.Exceptions.propertyValueCanNotBeNull()},
                    { null, "v", Property.Exceptions.propertyKeyCanNotBeNull()},
                    { Property.Key.ID, "v", Property.Exceptions.propertyKeyIdIsReserved()},
                    { Property.Key.LABEL, "v", Property.Exceptions.propertyKeyLabelIsReserved()},
                    { "", "v", Property.Exceptions.propertyKeyCanNotBeEmpty()}});
        }

        @Parameterized.Parameter(value = 0)
        public String key;

        @Parameterized.Parameter(value = 1)
        public String val;

        @Parameterized.Parameter(value = 2)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void testGraphVertexSetPropertyStandard() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.setProperty(key, val);
                fail(String.format("Call to Vertex.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void testGraphEdgeSetPropertyStandard() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v).setProperty(key, val);
                fail(String.format("Call to Edge.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_COMPUTER)
        public void testGraphVertexSetPropertyGraphComputer() throws Exception {
            try {
                this.g.addVertex();
                final Future future = g.compute().program(new MockVertexProgramForVertex(key, val)).submit();
                future.get();
                fail(String.format("Call to Vertex.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                final Throwable inner = ex.getCause();
                assertEquals(expectedException.getClass(), inner.getClass());
                assertEquals(expectedException.getMessage(), inner.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_COMPUTER)
        public void testGraphEdgeSetPropertyGraphComputer() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v);
                final Future future = g.compute().program(new MockVertexProgramForEdge(key, val)).submit();
                future.get();
                fail(String.format("Call to Edge.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                final Throwable inner = ex.getCause();
                assertEquals(expectedException.getClass(), inner.getClass());
                assertEquals(expectedException.getMessage(), inner.getMessage());
            }
        }
    }

    /**
     * Test exceptions around use of {@link Direction} with the incorrect context.
     */
    public static class UseOfDirectionTest extends AbstractBlueprintsTest {

        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Test
        public void testGetVertexOnEdge() {
            final Vertex v = g.addVertex();
            try {
                v.addEdge("label", v).getVertex(Direction.BOTH);
                tryCommit(g);
                fail("Call to Edge.getVertex(BOTH) should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Element.Exceptions.bothIsNotSupported();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }
    }

    /**
     * Tests specific to setting {@link Element} properties with
     * {@link com.tinkerpop.blueprints.computer.GraphComputer}.
     */
    public static class PropertyValidationOnSetGraphComputerTest extends AbstractBlueprintsTest {

        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Test
        public void testGraphVertexSetPropertyNoComputeKey() {
            final String key = "key-not-a-compute-key";
            try {
                this.g.addVertex();
                final Future future = g.compute()
                        .isolation(GraphComputer.Isolation.BSP)
                        .program(new MockVertexProgramForVertex(key, "anything")).submit();
                future.get();
                fail(String.format("Call to Vertex.setProperty should have thrown an exception with these arguments [%s, anything]", key));
            } catch (Exception ex) {
                final Throwable inner = ex.getCause();
                final Exception expectedException = GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
                assertEquals(expectedException.getClass(), inner.getClass());
                assertEquals(expectedException.getMessage(), inner.getMessage());
            }
        }

        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        @Test
        public void testGraphEdgeSetPropertyNoComputeKey() {
            final String key = "key-not-a-compute-key";
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v);
                final Future future = g.compute()
                        .isolation(GraphComputer.Isolation.BSP)
                        .program(new MockVertexProgramForEdge(key, "anything")).submit();
                future.get();
                fail(String.format("Call to Edge.setProperty should have thrown an exception with these arguments [%s, anything]", key));
            } catch (Exception ex) {
                final Throwable inner = ex.getCause();
                final Exception expectedException = GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
                assertEquals(expectedException.getClass(), inner.getClass());
                assertEquals(expectedException.getMessage(), inner.getMessage());
            }
        }

        // TODO: need to trigger this: GraphComputer.Exceptions.adjacentElementPropertiesCanNotBeWritten()
    }

    /**
     * Mock {@link VertexProgram} that just dummies up a way to set a property on a {@link Vertex}.
     */
    private static class MockVertexProgramForVertex implements VertexProgram {
        private final String key;
        private final String val;
        private final Map<String, KeyType> computeKeys = new HashMap<>();

        public MockVertexProgramForVertex(final String key, final String val) {
            this.key = key;
            this.val = val;
        }

        @Override
        public void setup(final GraphMemory graphMemory) {
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final GraphMemory graphMemory) {
            vertex.setProperty(this.key, this.val);
        }

        @Override
        public boolean terminate(GraphMemory graphMemory) {
            return true;
        }

        @Override
        public Map<String, KeyType> getComputeKeys() {
            return this.computeKeys;
        }
    }

    /**
     * Mock {@link VertexProgram} that just dummies up a way to set a property on an {@link Edge}.
     */
    private static class MockVertexProgramForEdge implements VertexProgram {
        private final String key;
        private final String val;
        private final Map<String, KeyType> computeKeys = new HashMap<>();

        public MockVertexProgramForEdge(final String key, final String val) {
            this.key = key;
            this.val = val;
        }

        @Override
        public void setup(final GraphMemory graphMemory) {
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final GraphMemory graphMemory) {
            vertex.query().edges().forEach(e->e.setProperty(this.key, this.val));
        }

        @Override
        public boolean terminate(GraphMemory graphMemory) {
            return true;
        }

        @Override
        public Map<String, KeyType> getComputeKeys() {
            return this.computeKeys;
        }
    }
}
