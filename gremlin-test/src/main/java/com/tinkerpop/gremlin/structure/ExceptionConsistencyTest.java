package com.tinkerpop.gremlin.structure;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.computer.ComputerResult;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.SideEffects;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.computer.lambda.LambdaVertexProgram;
import com.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.commons.configuration.Configuration;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_COMPUTER;
import static com.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures.FEATURE_TRANSACTIONS;
import static com.tinkerpop.gremlin.structure.Graph.Features.PropertyFeatures.FEATURE_PROPERTIES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VariableFeatures.FEATURE_VARIABLES;
import static com.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures.FEATURE_USER_SUPPLIED_IDS;
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
     * Test for consistent exceptions for graphs not supporting user supplied identifiers.
     */
    @ExceptionCoverage(exceptionClass = Edge.Exceptions.class, methods = {
            "userSuppliedIdsNotSupported"
    })
    @ExceptionCoverage(exceptionClass = Vertex.Exceptions.class, methods = {
            "userSuppliedIdsNotSupported"
    })
    public static class AddElementWithIdTest extends AbstractGremlinTest {
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
     * Checks that properties added to an {@link com.tinkerpop.gremlin.structure.Element} are validated in a consistent way when they are set after
     * {@link com.tinkerpop.gremlin.structure.Vertex} or {@link com.tinkerpop.gremlin.structure.Edge} construction by throwing an appropriate exception.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyValueCanNotBeNull",
            "propertyKeyCanNotBeNull",
            "propertyKeyIdIsReserved",
            "propertyKeyLabelIsReserved",
            "propertyKeyCanNotBeEmpty"
    })
    public static class PropertyValidationOnSetTest extends AbstractGremlinTest {

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
                v.property(key, val);
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
                v.addEdge("label", v).property(key, val);
                fail(String.format("Call to Edge.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        // todo: fix this test around graph computer
        @Test
        @Ignore
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_COMPUTER)
        public void testGraphVertexSetPropertyGraphComputer() throws Exception {
            try {
                this.g.addVertex();
                final Future future = g.compute()
                        //.program(new MockVertexProgramForVertex(key, val))
                        .submit();
                future.get();
                fail(String.format("Call to Vertex.setProperty should have thrown an exception with these arguments [%s, %s]", key, val));
            } catch (Exception ex) {
                final Throwable inner = ex.getCause();
                assertEquals(expectedException.getClass(), inner.getClass());
                assertEquals(expectedException.getMessage(), inner.getMessage());
            }
        }

        // todo: fix this test around graph computer
        @Test
        @Ignore
        @FeatureRequirement(featureClass = Graph.Features.EdgePropertyFeatures.class, feature = FEATURE_PROPERTIES)
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_COMPUTER)
        public void testGraphEdgeSetPropertyGraphComputer() throws Exception {
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v);
                final Future future = g.compute()
                        //.program(new MockVertexProgramForEdge(key, val))
                        .submit();
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
     * Test exceptions around {@link com.tinkerpop.gremlin.structure.Graph.Variables}.
     */
    @RunWith(Parameterized.class)
    @ExceptionCoverage(exceptionClass = Graph.Variables.Exceptions.class, methods = {
            "variableValueCanNotBeNull",
            "variableKeyCanNotBeNull",
            "variableKeyCanNotBeEmpty"
    })
    public static class VariableTest extends AbstractGremlinTest {
        @Parameterized.Parameters(name = "{index}: expect - {2}")
        public static Iterable<Object[]> data() {
            return Arrays.asList(new Object[][]{
                    {"k", null, Graph.Variables.Exceptions.variableValueCanNotBeNull()},
                    {null, "v", Graph.Variables.Exceptions.variableKeyCanNotBeNull()},
                    {"", "v", Graph.Variables.Exceptions.variableKeyCanNotBeEmpty()}});
        }

        @Parameterized.Parameter(value = 0)
        public String key;

        @Parameterized.Parameter(value = 1)
        public String val;

        @Parameterized.Parameter(value = 2)
        public Exception expectedException;

        @Test
        @FeatureRequirement(featureClass = Graph.Features.VariableFeatures.class, feature = FEATURE_VARIABLES)
        public void testGraphAnnotationsSet() throws Exception {
            try {
                g.variables().set(key, val);
                fail(String.format("Setting an annotation with these arguments [key: %s value: %s] should throw an exception", key, val));
            } catch (Exception ex) {
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Addition of an {@link com.tinkerpop.gremlin.structure.Edge} without a label should throw an exception.
     */
    @ExceptionCoverage(exceptionClass = Edge.Exceptions.class, methods = {
            "edgeLabelCanNotBeNull"
    })
    public static class EdgeLabelTest extends AbstractGremlinTest {
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
     * Tests around exceptions when working with {@link com.tinkerpop.gremlin.structure.Transaction}.
     */
    @ExceptionCoverage(exceptionClass = Transaction.Exceptions.class, methods = {
            "transactionAlreadyOpen",
            "threadedTransactionsNotSupported",
            "openTransactionsOnClose",
            "transactionMustBeOpenToReadWrite",
            "onCloseBehaviorCannotBeNull",
            "onReadWriteBehaviorCannotBeNull"
    })
    public static class TransactionTest extends AbstractGremlinTest {

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

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
        public void testTransactionOpenOnClose() {
            g.tx().onClose(Transaction.CLOSE_BEHAVIOR.MANUAL);

            if (!g.tx().isOpen())
                g.tx().open();

            try {
                g.close();
                fail("An exception should be thrown when close behavior is manual and the graph is close with an open transaction");
            } catch (Exception ex) {
                final Exception expectedException = Transaction.Exceptions.openTransactionsOnClose();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
        public void testManualTransaction() {
            g.tx().onReadWrite(Transaction.READ_WRITE_BEHAVIOR.MANUAL);

            try {
                g.addVertex();
                fail("An exception should be thrown when read/write behavior is manual and no transaction is opened");
            } catch (Exception ex) {
                final Exception expectedException = Transaction.Exceptions.transactionMustBeOpenToReadWrite();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
        public void testOnCloseToNull() {
            try {
                g.tx().onClose(null);
                fail("An exception should be thrown when onClose behavior is set to null");
            } catch (Exception ex) {
                final Exception expectedException = Transaction.Exceptions.onCloseBehaviorCannotBeNull();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = FEATURE_TRANSACTIONS)
        public void testOnReadWriteToNull() {
            try {
                g.tx().onReadWrite(null);
                fail("An exception should be thrown when onClose behavior is set to null");
            } catch (Exception ex) {
                final Exception expectedException = Transaction.Exceptions.onReadWriteBehaviorCannotBeNull();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }
    }

    /**
     * Test exceptions where the same ID is assigned twice to an {@link com.tinkerpop.gremlin.structure.Element},
     */
    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "vertexWithIdAlreadyExists",
            "edgeWithIdAlreadyExists"
    })
    public static class SameIdUsageTest extends AbstractGremlinTest {
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
                final Exception expectedException = Graph.Exceptions.edgeWithIdAlreadyExists(1000l);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }
    }

    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "elementNotFound"
    })
    public static class GraphFindElement extends AbstractGremlinTest {
        @Test
        public void testFindVertexByIdWithNull() {
            try {
                g.v(null);
                fail("Call to g.v(null) should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Graph.Exceptions.elementNotFound(Vertex.class, null);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }

        @Test
        public void testFindEdgeByIdWithNull() {
            try {
                g.e(null);
                fail("Call to g.e(null) should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Graph.Exceptions.elementNotFound(Edge.class, null);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }

        @Test
        public void testFindVertexByIdThatIsNonExistent() {
            try {
                g.v(10000l);
                fail("Call to g.v(null) should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Graph.Exceptions.elementNotFound(Vertex.class, 10000l);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }

        @Test
        public void testFindEdgeByIdThatIsNonExistent() {
            try {
                g.e(10000l);
                fail("Call to g.e(null) should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Graph.Exceptions.elementNotFound(Edge.class, 10000l);
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }
    }

    /**
     * Test exceptions around use of {@link com.tinkerpop.gremlin.structure.Element#value(String)}.
     */
    @ExceptionCoverage(exceptionClass = Property.Exceptions.class, methods = {
            "propertyDoesNotExist"
    })
    public static class ElementGetValueTest extends AbstractGremlinTest {
        @Test
        @FeatureRequirement(featureClass = Graph.Features.VertexPropertyFeatures.class, feature = FEATURE_PROPERTIES)
        public void testGetValueThatIsNotPresentOnVertex() {
            final Vertex v = g.addVertex();
            try {
                v.value("does-not-exist");
                fail("Call to Element.value() with a key that is not present should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Property.Exceptions.propertyDoesNotExist("does-not-exist");
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
                e.value("does-not-exist");
                fail("Call to Element.value() with a key that is not present should throw an exception");
            } catch (Exception ex) {
                final Exception expectedException = Property.Exceptions.propertyDoesNotExist("does-not-exist");
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }

        }
    }

    /**
     * Tests specific to setting {@link com.tinkerpop.gremlin.structure.Element} properties with
     * {@link com.tinkerpop.gremlin.process.computer.GraphComputer}.
     */
    @ExceptionCoverage(exceptionClass = GraphComputer.Exceptions.class, methods = {
            "providedKeyIsNotAComputeKey",
            "computerHasNoVertexProgramNorMapReducers",
            "computerHasAlreadyBeenSubmittedAVertexProgram"
    })
    @ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
            "graphDoesNotSupportProvidedGraphComputer",
            "onlyOneOrNoGraphComputerClass"
    })
    public static class GraphComputerTest extends AbstractGremlinTest {

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
        public void shouldOnlyAllowOneOrNoGraphComputerClass() throws Exception {
            try {
                g.compute(BadGraphComputer.class, BadGraphComputer.class).submit().get();
                fail("Should throw an IllegalArgument when two graph computers are passed in");
            } catch (Exception ex) {
                final Exception expectedException = Graph.Exceptions.onlyOneOrNoGraphComputerClass();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @LoadGraphWith(CLASSIC)
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
        public void shouldNotAllowWithNoVertexProgramNorMapReducers() throws Exception {
            try {
                g.compute().submit().get();
                fail("Should throw an IllegalStateException when there is no vertex program nor map reducers");
            } catch (Exception ex) {
                final Exception expectedException = GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
                assertEquals(expectedException.getClass(), ex.getClass());
                assertEquals(expectedException.getMessage(), ex.getMessage());
            }
        }

        @Test
        @LoadGraphWith(CLASSIC)
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
        public void shouldNotAllowTheSameComputerToExecutedTwice() throws Exception {
            final Supplier<VertexProgram> identity = () -> LambdaVertexProgram.build().
                    setup(s -> {
                    }).
                    execute((v, m, s) -> {
                    }).
                    terminate(s -> true).create();
            final GraphComputer computer = g.compute().program(identity.get());
            computer.submit().get(); // this should work as its the first run of the graph computer

            try {
                computer.submit(); // this should fail as the computer has already been executed
                fail("Using the same graph computer to compute again should not be possible");
            } catch (IllegalStateException e) {
                assertTrue(true);
            } catch (Exception e) {
                fail("Should yield an illegal state exception for graph computer being executed twice");
            }

            computer.program(identity.get());
            // test no rerun of graph computer
            try {
                computer.submit(); // this should fail as the computer has already been executed even through new program submitted
                fail("Using the same graph computer to compute again should not be possible");
            } catch (IllegalStateException e) {
                assertTrue(true);
            } catch (Exception e) {
                fail("Should yield an illegal state exception for graph computer being executed twice");
            }
        }

        @Test
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
        public void shouldNotAllowBadGraphComputers() {
            try {
                g.compute(BadGraphComputer.class);
                fail("Providing a bad graph computer class should fail");
            } catch (IllegalArgumentException e) {
                assertTrue(true);
            } catch (Exception e) {
                fail("Should provide an IllegalArgumentException");
            }
        }


        @Test
        @Ignore
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
        public void testGraphVertexSetPropertyNoComputeKey() {
            final String key = "key-not-a-compute-key";
            try {
                this.g.addVertex();
                final Future future = g.compute()
                        .isolation(GraphComputer.Isolation.BSP)
                                //.program(new MockVertexProgramForVertex(key, "anything"))
                        .submit();
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
        @Ignore
        @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
        public void testGraphEdgeSetPropertyNoComputeKey() {
            final String key = "key-not-a-compute-key";
            try {
                final Vertex v = this.g.addVertex();
                v.addEdge("label", v);
                final Future future = g.compute()
                        .isolation(GraphComputer.Isolation.BSP)
                                //.program(new MockVertexProgramForEdge(key, "anything"))
                        .submit();
                future.get();
                fail(String.format("Call to Edge.setProperty should have thrown an exception with these arguments [%s, anything]", key));
            } catch (Exception ex) {
                final Throwable inner = ex.getCause();
                final Exception expectedException = GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
                assertEquals(expectedException.getClass(), inner.getClass());
                assertEquals(expectedException.getMessage(), inner.getMessage());
            }
        }

        class BadGraphComputer implements GraphComputer {
            public GraphComputer isolation(final Isolation isolation) {
                return null;
            }

            public GraphComputer program(final VertexProgram vertexProgram) {
                return null;
            }

            public GraphComputer mapReduce(final MapReduce mapReduce) {
                return null;
            }

            public Future<ComputerResult> submit() {
                return null;
            }
        }
    }

    private static class MockVertexProgramBuilder implements VertexProgram.Builder {
        @Override
        public <P extends VertexProgram> P create() {
            return (P) PageRankVertexProgram.build().create();
        }

        @Override
        public VertexProgram.Builder configure(final Object... keyValues) {
            return this;
        }
    }

    /**
     * Mock {@link com.tinkerpop.gremlin.process.computer.VertexProgram} that just dummies up a way to set a property on a {@link com.tinkerpop.gremlin.structure.Vertex}.
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
        public void loadState(final Configuration configuration) {
        }

        @Override
        public void storeState(final Configuration configuration) {
        }

        @Override
        public void setup(final SideEffects sideEffects) {
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final SideEffects sideEffects) {
            vertex.property(this.key, this.val);
        }

        @Override
        public boolean terminate(SideEffects sideEffects) {
            return true;
        }

        @Override
        public Set<String> getSideEffectComputeKeys() {
            return Collections.emptySet();
        }

        @Override
        public Map<String, KeyType> getElementComputeKeys() {
            return this.computeKeys;
        }
    }

    /**
     * Mock {@link com.tinkerpop.gremlin.process.computer.VertexProgram} that just dummies up a way to set a property on an {@link com.tinkerpop.gremlin.structure.Edge}.
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
        public void storeState(final Configuration configuration) {
        }

        @Override
        public void loadState(final Configuration configuration) {
        }

        @Override
        public void setup(final SideEffects sideEffects) {
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final SideEffects sideEffects) {
            vertex.bothE().forEach(e -> e.<String>property(this.key, this.val));
        }

        @Override
        public boolean terminate(SideEffects sideEffects) {
            return true;
        }

        @Override
        public Set<String> getSideEffectComputeKeys() {
            return Collections.emptySet();
        }

        @Override
        public Map<String, KeyType> getElementComputeKeys() {
            return this.computeKeys;
        }
    }
}
