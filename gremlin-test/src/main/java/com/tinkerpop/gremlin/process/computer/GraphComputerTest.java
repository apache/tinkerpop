package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.computer.lambda.LambdaVertexProgram;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.Future;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphComputerTest extends AbstractGremlinTest {

    @Test
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
    public void shouldHaveStandardStringRepresentation() {
        final GraphComputer computer = g.compute();
        assertEquals(StringFactory.computerString(computer), computer.toString());
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
    @LoadGraphWith(CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
    public void shouldHaveConsistentSideEffectsAndExceptions() throws Exception {
        GraphComputer computer = g.compute();
        ComputerResult results = computer.program(LambdaVertexProgram.build().
                execute((v, m, s) -> {
                    if (s.isInitialIteration()) {
                        v.property("nameLengthCounter", v.<String>value("name").length());
                        s.incr("counter", 1l);
                    } else
                        v.property("nameLengthCounter", v.<String>value("name").length() + v.<Integer>value("nameLengthCounter"));
                }).
                terminate(s -> s.getIteration() >= 2).
                elementComputeKeys("nameLengthCounter", VertexProgram.KeyType.VARIABLE).
                sideEffectKeys(new HashSet<>(Arrays.asList("counter"))).create()).submit().get();
        assertEquals(1, results.getSideEffects().getIteration());
        assertEquals(1, results.getSideEffects().asMap().size());
        assertEquals(Long.valueOf(6), results.getSideEffects().<Long>get("counter").get());
        assertEquals(Long.valueOf(6), results.getGraph().V().count().next());
        results.getGraph().V().forEach(v -> {
            assertTrue(v.property("nameLengthCounter").isPresent());
            assertEquals(Integer.valueOf(v.<String>value("name").length() * 2), Integer.valueOf(v.<Integer>value("nameLengthCounter")));
        });
        // test no rerun of graph computer
        try {
            computer.submit();
            fail("Using the same graph computer to compute again should not be possible");
        } catch (IllegalStateException e) {
            assertTrue(true);
        } catch (Exception e) {
            fail("Should yield an illegal state exception for graph computer being executed twice");
        }
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
    public void shouldProvideOptionalEmptySideEffects() throws Exception {
        ComputerResult results = g.compute().program(LambdaVertexProgram.build().
                        execute((v, m, s) -> v.<Integer>property("counter", s.isInitialIteration() ? 1 : v.<Integer>value("counter") + 1)).
                        terminate(s -> s.getIteration() > 9).
                        elementComputeKeys("counter", VertexProgram.KeyType.VARIABLE).create()).submit().get();
        results.getGraph().V().value("counter").forEach(System.out::println);

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
