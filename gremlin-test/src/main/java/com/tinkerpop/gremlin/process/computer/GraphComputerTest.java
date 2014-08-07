package com.tinkerpop.gremlin.process.computer;

import com.tinkerpop.gremlin.AbstractGremlinTest;
import com.tinkerpop.gremlin.LoadGraphWith;
import com.tinkerpop.gremlin.process.computer.ranking.SimpleVertexProgram;
import com.tinkerpop.gremlin.structure.FeatureRequirement;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.junit.Test;

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
    public void shouldExecuteSimpleVertexProgram() throws Exception {
        GraphComputer computer = g.compute();
        ComputerResult results = computer.program(new SimpleVertexProgram()).submit().get();
        assertEquals(1, results.getSideEffects().getIteration());
        assertEquals(results.getSideEffects().asMap().size(), 0);
        assertEquals(Long.valueOf(6), results.getGraph().V().count().next());
        results.getGraph().V().forEach(v -> {
            assertTrue(v.property(SimpleVertexProgram.COUNTER).isPresent());
            assertEquals(Integer.valueOf(v.<String>value("name").length() * 2), Integer.valueOf(v.<Integer>value(SimpleVertexProgram.COUNTER)));
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
}
