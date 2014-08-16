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
