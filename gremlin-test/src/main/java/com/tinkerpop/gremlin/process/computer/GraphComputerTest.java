package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.computer.lambda.LambdaMapReduce;
import com.tinkerpop.gremlin.process.computer.lambda.LambdaVertexProgram;
import com.tinkerpop.gremlin.structure.ExceptionCoverage;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.tinkerpop.gremlin.LoadGraphWith.GraphData.CLASSIC;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@ExceptionCoverage(exceptionClass = GraphComputer.Exceptions.class, methods = {
        "providedKeyIsNotASideEffectKey",
})
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
    public void shouldNotAllowTheSameComputerToExecutedTwice() throws Exception {
        final GraphComputer computer = g.compute().program(identity());
        computer.submit().get(); // this should work as its the first run of the graph computer

        try {
            computer.submit(); // this should fail as the computer has already been executed
            fail("Using the same graph computer to compute again should not be possible");
        } catch (IllegalStateException e) {
            assertTrue(true);
        } catch (Exception e) {
            fail("Should yield an illegal state exception for graph computer being executed twice");
        }

        computer.program(identity());
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
    @Ignore
    @LoadGraphWith(CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
    public void shouldRequireRegisteringSideEffectKeys() throws Exception {
        try {
            g.compute().program(LambdaVertexProgram.build().
                    setup(s -> s.set("or", true)).
                    execute((v, m, s) -> {
                    }).
                    terminate(s -> s.getIteration() >= 2).create()).submit().get();
            // TODO: Giraph fails HARD and kills the process (thus, test doesn't proceed past this point)
            fail("Should fail with an ExecutionException[IllegalArgumentException]");
        } catch (ExecutionException e) {
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        } catch (Exception e) {
            fail("Should fail with an ExecutionException[IllegalArgumentException]: " + e);
        }
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
    public void shouldAllowMapReduceWithNoVertexProgram() throws Exception {
        final ComputerResult results = g.compute().mapReduce(LambdaMapReduce.<MapReduce.NullObject, Integer, MapReduce.NullObject, Integer, Integer>build()
                .map((v, e) -> v.<Integer>property("age").ifPresent(age -> e.emit(MapReduce.NullObject.instance(), age)))
                .reduce((k, vv, e) -> e.emit(MapReduce.NullObject.instance(), StreamFactory.stream(vv).mapToInt(i -> i).sum()))
                .sideEffect(i -> i.next().getValue1())
                .sideEffectKey("ageSum").create())
                .submit().get();
        assertEquals(123, results.getSideEffects().get("ageSum").get());
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
    public void shouldHaveConsistentSideEffectsAndExceptions() throws Exception {
        GraphComputer computer = g.compute();
        ComputerResult results = computer.program(LambdaVertexProgram.build().
                setup(s -> s.set("or", true)).
                execute((v, m, s) -> {
                    if (s.isInitialIteration()) {
                        v.property("nameLengthCounter", v.<String>value("name").length());
                        s.incr("counter", v.<String>value("name").length());
                        s.and("and", v.<String>value("name").length() == 5);
                        s.and("or", false);
                    } else
                        v.property("nameLengthCounter", v.<String>value("name").length() + v.<Integer>value("nameLengthCounter"));
                }).
                terminate(s -> s.getIteration() >= 2).
                elementComputeKeys("nameLengthCounter", VertexProgram.KeyType.VARIABLE).
                sideEffectKeys(new HashSet<>(Arrays.asList("counter", "and", "or"))).create()).submit().get();
        assertEquals(1, results.getSideEffects().getIteration());
        assertEquals(3, results.getSideEffects().asMap().size());
        assertEquals(3, results.getSideEffects().keys().size());
        assertTrue(results.getSideEffects().keys().contains("counter"));
        assertTrue(results.getSideEffects().keys().contains("and"));
        assertTrue(results.getSideEffects().keys().contains("or"));
        assertTrue(results.getSideEffects().getRuntime() > 0);

        assertEquals(Long.valueOf(28), results.getSideEffects().<Long>get("counter").get());
        assertFalse(results.getSideEffects().<Boolean>get("and").get());
        assertFalse(results.getSideEffects().<Boolean>get("or").get());
        assertFalse(results.getSideEffects().get("BAD").isPresent());
        assertEquals(Long.valueOf(6), results.getGraph().V().count().next());

        results.getGraph().V().forEach(v -> {
            assertTrue(v.property("nameLengthCounter").isPresent());
            assertEquals(Integer.valueOf(v.<String>value("name").length() * 2), Integer.valueOf(v.<Integer>value("nameLengthCounter")));
        });
    }

    @Test
    @LoadGraphWith(CLASSIC)
    @FeatureRequirement(featureClass = Graph.Features.GraphFeatures.class, feature = Graph.Features.GraphFeatures.FEATURE_COMPUTER)
    public void shouldSupportMultipleMapReduceJobs() throws Exception {
        final ComputerResult results = g.compute().program(LambdaVertexProgram.build()
                .execute((v, m, s) -> v.<Integer>property("counter", s.isInitialIteration() ? 1 : v.<Integer>value("counter") + 1))
                .terminate(s -> s.getIteration() > 9)
                .elementComputeKeys("counter", VertexProgram.KeyType.VARIABLE).create())
                .mapReduce(LambdaMapReduce.<MapReduce.NullObject, Integer, MapReduce.NullObject, Integer, Integer>build()
                        .map((v, e) -> e.emit(MapReduce.NullObject.instance(), v.value("counter")))
                        .reduce((k, vv, e) -> {
                            int counter = 0;
                            while (vv.hasNext()) {
                                counter = counter + vv.next();
                            }
                            e.emit(MapReduce.NullObject.instance(), counter);

                        })
                        .sideEffect(i -> i.next().getValue1())
                        .sideEffectKey("a").create())
                .mapReduce(LambdaMapReduce.<MapReduce.NullObject, Integer, MapReduce.NullObject, Integer, Integer>build()
                        .map((v, e) -> e.emit(MapReduce.NullObject.instance(), v.value("counter")))
                        .combine((k, vv, e) -> e.emit(MapReduce.NullObject.instance(), 1))
                        .reduce((k, vv, e) -> e.emit(MapReduce.NullObject.instance(), 1))
                        .sideEffect(i -> i.next().getValue1())
                        .sideEffectKey("b").create())
                .submit().get();


        assertEquals(60, results.getSideEffects().get("a").get());
        assertEquals(1, results.getSideEffects().get("b").get());
    }

    private static LambdaVertexProgram identity() {
        return LambdaVertexProgram.build().
                setup(s -> {
                }).
                execute((v, m, s) -> {
                }).
                terminate(s -> true).create();
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
