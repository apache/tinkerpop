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

import static com.tinkerpop.blueprints.Graph.Features.GraphAnnotationFeatures.FEATURE_ANNOTATIONS;
import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_COMPUTER;
import static com.tinkerpop.blueprints.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.blueprints.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static com.tinkerpop.blueprints.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
import static org.junit.Assert.*;

/**
 * Ensure that exception handling is consistent within Blueprints. It may be necessary to throw exceptions in an
 * appropriate order in order to ensure that these tests pass.  Note that some exception consistency checks are
 * in the {@link FeatureSupportTest}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Enclosed.class)
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class ExceptionConsistencyTest {

    /**
     * Checks that properties added to an {@link Element} are validated in a consistent way when they are added at
     * {@link Vertex} or {@link Edge} construction by throwing an appropriate exception.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "providedKeyValuesMustBeAMultipleOfTwo",
            "providedKeyValuesMustHaveALegalKeyOnEvenIndices"
    })
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyValueCanNotBeNull",
            "propertyKeyCanNotBeEmpty"
    })
    public static class PropertyValidationOnAddTest extends AbstractBlueprintsTest {

        @Parameterized.Parameters(name = "{index}: expect - {1}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {new Object[]{"odd", "number", "arguments"}, Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    {new Object[]{"odd"}, Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    {new Object[]{"odd", "number", 123, "test"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    {new Object[]{"odd", null}, Property.Exceptions.propertyValueCanNotBeNull()},
                    {new Object[]{null, "val"}, Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices()},
                    {new Object[]{"", "val"}, Property.Exceptions.propertyKeyCanNotBeEmpty()}});
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
     * Test for consistent exceptions for graphs not supporting user supplied identifiers.
     */
    @ExceptionCoverage(exceptionClass = Edge.Exceptions.class, methods = {
            "userSuppliedIdsNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Vertex.Exceptions.class, methods = {
            "userSuppliedIdsNotSupported"
    })
    public static class AddElementWithIdTest extends AbstractBlueprintsTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void testGraphAddVertex() throws Exception {
            try {
                this.g.addVertex(Element.ID, "");
                fail("Call to addVertex should have thrown an exception when ID was specified as it is not supported");
            } catch (Exception ex) {
                final Exception expectedException = Vertex.Exceptions.userSuppliedIdsNotSupported();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS, supported = false)
        public void testGraphAddEdge() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v, Element.ID, "");
                fail("Call to addEdge should have thrown an exception when ID was specified as it is not supported");
            } catch (Exception ex) {
                final Exception expectedException = Edge.Exceptions.userSuppliedIdsNotSupported();
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
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyValueCanNotBeNull",
            "propertyKeyCanNotBeNull",
            "propertyKeyIdIsReserved",
            "propertyKeyLabelIsReserved",
            "propertyKeyCanNotBeEmpty"
    })
    public static class PropertyValidationOnSetTest extends AbstractBlueprintsTest {

        @Parameterized.Parameters(name = "{index}: expect - {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"k", null, Property.Exceptions.propertyValueCanNotBeNull()},
                    {null, "v", Property.Exceptions.propertyKeyCanNotBeNull()},
                    {Element.ID, "v", Property.Exceptions.propertyKeyIdIsReserved()},
                    {Element.LABEL, "v", Property.Exceptions.propertyKeyLabelIsReserved()},
                    {"", "v", Property.Exceptions.propertyKeyCanNotBeEmpty()}});
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
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "bothIsNotSupported"
    })
    public static class UseOfDirectionTest extends AbstractBlueprintsTest {
        @Test
        public void testGetVertexOnEdge() {
            final Vertex v = g.addVertex();
            try {
                v.addEdge("label", v).getVertex(Direction.BOTH);
                fail("Call to Edge.getVertex(BOTH) should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Element.Exceptions.bothIsNotSupported();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }
    }

    /**
     * Test exceptions around {@link Graph.Annotations}.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Graph.Annotations.Exceptions.class, methods = {
            "graphAnnotationValueCanNotBeNull",
            "graphAnnotationKeyCanNotBeNull",
            "annotationKeyValueIsReserved",
            "graphAnnotationKeyCanNotBeEmpty"
    })
    public static class GraphAnnotationsTest extends AbstractBlueprintsTest {
        @Parameterized.Parameters(name = "{index}: expect - {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"k", null, Graph.Annotations.Exceptions.graphAnnotationValueCanNotBeNull()},
                    {null, "v", Graph.Annotations.Exceptions.graphAnnotationKeyCanNotBeNull()},
                    {"", "v", Graph.Annotations.Exceptions.graphAnnotationKeyCanNotBeEmpty()}});
        }

        @Parameterized.Parameter(value = 0)
        public String key;

        @Parameterized.Parameter(value = 1)
        public String val;

        @Parameterized.Parameter(value = 2)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphAnnotationFeatures.class, feature = FEATURE_ANNOTATIONS)
        public void testGraphAnnotationsSet() throws Exception {
            try {
                g.annotations().set(key, val);
                fail(String.format("Setting an annotation with these arguments [key: %s value: %s] should throw an exception", key, val));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Tests for exceptions with {@link AnnotatedValue}.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = AnnotatedValue.Exceptions.class, methods = {
            "providedKeyValuesMustBeAMultipleOfTwo",
            "providedKeyValuesMustHaveAStringOnEvenIndices",
            "annotationValueCanNotBeNull",
            "annotationKeyValueIsReserved",
            "annotationKeyCanNotBeEmpty",
            "annotationKeyCanNotBeNull",
            "annotatedValueCanNotBeNull"
    })
    public static class AnnotatedValueTest extends AbstractBlueprintsTest {

        @Parameterized.Parameters(name = "{index}: expect - {0,1}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {null, new Object[]{"good", "odd", "number", "arguments"}, AnnotatedValue.Exceptions.annotatedValueCanNotBeNull()},
                    {"something", new Object[]{"odd", "number", "arguments"}, AnnotatedValue.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    {"something", new Object[]{"odd"}, AnnotatedValue.Exceptions.providedKeyValuesMustBeAMultipleOfTwo()},
                    {"something", new Object[]{"odd", "number", 123, "test"}, AnnotatedValue.Exceptions.providedKeyValuesMustHaveAStringOnEvenIndices()},
                    {"something", new Object[]{"odd", null}, AnnotatedValue.Exceptions.annotationValueCanNotBeNull()},
                    {"something", new Object[]{null, "val"}, AnnotatedValue.Exceptions.providedKeyValuesMustHaveAStringOnEvenIndices()},
                    {"something", new Object[]{(String) null, "val"}, AnnotatedValue.Exceptions.providedKeyValuesMustHaveAStringOnEvenIndices()},
                    {"something", new Object[]{AnnotatedValue.VALUE, "v"}, AnnotatedValue.Exceptions.annotationKeyValueIsReserved()},
                    {"something", new Object[]{"", "val"}, AnnotatedValue.Exceptions.annotationKeyCanNotBeEmpty()}});
        }

        @Parameterized.Parameter(value = 0)
        public String annotatedValue;

        @Parameterized.Parameter(value = 1)
        public Object[] arguments;

        @Parameterized.Parameter(value = 2)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexAnnotationFeatures.class, feature = FEATURE_ANNOTATIONS)
        public void testAnnotatedListAddValue() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.setProperty("names", AnnotatedList.make());
                final Property<AnnotatedList<String>> names = v.getProperty("names");
                names.get().addValue(annotatedValue, arguments);
                fail(String.format("Call to addValue should have thrown an exception with these arguments [%s]", arguments));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Addition of an {@link Edge} without a label should throw an exception.
     */
    @ExceptionCoverage(exceptionClass = Edge.Exceptions.class, methods = {
            "edgeLabelCanNotBeNull"
    })
    public static class EdgeLabelTest extends AbstractBlueprintsTest {
        @Test
        public void testNullEdgeLabel() {
            final Vertex v = g.addVertex();
            try {
                v.addEdge(null, v);
                fail("Call to Vertex.addEdge() should throw an exception when label is null");
            } catch (Exception ex) {
                final Exception expectedException = Edge.Exceptions.edgeLabelCanNotBeNull();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Tests around exceptions when working with {@link Transaction}.
     */
    @ExceptionCoverage(exceptionClass = Transaction.Exceptions.class, methods = {
            "transactionAlreadyOpen"
    })
    public static class TransactionTest extends AbstractBlueprintsTest {

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
        public void testTransactionAlreadyOpen() {
            if (!g.tx().isOpen())
                g.tx().open();

            try {
                g.tx().open();
                fail("An exception should be thrown when a transaction is opened twice");
            } catch (Exception ex) {
                final Exception expectedException = Transaction.Exceptions.transactionAlreadyOpen();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

    }

    /**
     * Test exceptions where the same ID is assigned twice to an {@link Element},
     */
    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "vertexWithIdAlreadyExists",
            "edgeWithIdAlreadyExist"
    })
    public static class SameIdUsageTest extends AbstractBlueprintsTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS)
        public void testAssignSameIdOnVertex() {
            g.addVertex(Element.ID, 1000l);
            try {
                g.addVertex(Element.ID, 1000l);
                fail("Assigning the same ID to an Element should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Graph.Exceptions.vertexWithIdAlreadyExists(1000l);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.EdgeFeatures.class, feature = Graph.Features.EdgeFeatures.FEATURE_USER_SUPPLIED_IDS)
        public void testAssignSameIdOnEdge() {
            final Vertex v = g.addVertex();
            v.addEdge("label", v, Element.ID, 1000l);

            try {
                v.addEdge("label", v, Element.ID, 1000l);
                fail("Assigning the same ID to an Element should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Graph.Exceptions.edgeWithIdAlreadyExist(1000l);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }
    }

    /**
     * Test exceptions around use of {@link Element#getValue(String)}.
     */
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyDoesNotExist"
    })
    public static class ElementGetValueTest extends AbstractBlueprintsTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void testGetValueThatIsNotPresentOnVertex() {
            final Vertex v = g.addVertex();
            try {
                v.getValue("does-not-exist");
                fail("Call to Element.getValue() with a key that is not present should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Property.Exceptions.propertyDoesNotExist();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void testGetValueThatIsNotPresentOnEdge() {
            final Vertex v = g.addVertex();
            final Edge e = v.addEdge("label", v);
            try {
                e.getValue("does-not-exist");
                fail("Call to Element.getValue() with a key that is not present should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Property.Exceptions.propertyDoesNotExist();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }
    }

    /**
     * An {@link Element} can only be removed once.
     */
    @ExceptionCoverage(exceptionClass = Element.Exceptions.class, methods = {
            "elementHasAlreadyBeenRemovedOrDoesNotExist"
    })
    public static class DuplicateRemovalTest extends AbstractBlueprintsTest {
        @Test
        public void shouldCauseExceptionIfEdgeRemovedMoreThanOnce() {
            final Vertex v1 = g.addVertex();
            final Vertex v2 = g.addVertex();
            Edge e = v1.addEdge("knows", v2);

            assertNotNull(e);

            Object id = e.getId();
            e.remove();
            assertFalse(g.query().ids(id).edges().iterator().hasNext());

            // try second remove with no commit
            try {
                e.remove();
                fail("Edge cannot be removed twice.");
            } catch (Exception ex) {
                final Exception expectedException = Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, id);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

            e = v1.addEdge("knows", v2);
            assertNotNull(e);

            id = e.getId();
            e.remove();

            // try second remove with a commit and then a second remove.  both should return the same exception
            tryCommit(g);

            try {
                e.remove();
                fail("Edge cannot be removed twice.");
            } catch (Exception ex) {
                final Exception expectedException = Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, id);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        public void shouldCauseExceptionIfVertexRemovedMoreThanOnce() {
            Vertex v = g.addVertex();
            assertNotNull(v);

            Object id = v.getId();
            v.remove();
            assertFalse(g.query().ids(id).vertices().iterator().hasNext());

            // try second remove with no commit
            try {
                v.remove();
                fail("Vertex cannot be removed twice.");
            } catch (Exception ex) {
                final Exception expectedException = Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, id);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

            v = g.addVertex();
            assertNotNull(v);

            id = v.getId();
            v.remove();

            // try second remove with a commit and then a second remove.  both should return the same exception
            tryCommit(g);

            try {
                v.remove();
                fail("Vertex cannot be removed twice.");
            } catch (Exception ex) {
                final Exception expectedException = Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, id);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Tests specific to setting {@link Element} properties with
     * {@link com.tinkerpop.blueprints.computer.GraphComputer}.
     */
    @ExceptionCoverage(exceptionClass = GraphComputer.Exceptions.class, methods = {
            "providedKeyIsNotAComputeKey"
    })
    public static class PropertyValidationOnSetGraphComputerTest extends AbstractBlueprintsTest {

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
            vertex.query().edges().forEach(e -> e.setProperty(this.key, this.val));
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
