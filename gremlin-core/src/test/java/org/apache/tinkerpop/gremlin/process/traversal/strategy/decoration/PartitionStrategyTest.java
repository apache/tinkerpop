package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.__;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.AddEdgeStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.graph.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Contains;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class PartitionStrategyTest {
    private static Traversal traversalWithAddV;
    static {
        final Graph mockedGraph = mock(Graph.class);
        final DefaultGraphTraversal t = new DefaultGraphTraversal<>(mockedGraph);
        t.asAdmin().addStep(new GraphStep<>(t.asAdmin(), Vertex.class));
        traversalWithAddV = t.addV();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"bothV()", __.bothV(), 1, false},
                {"inV()", __.inV(), 1, false},
                {"outV()", __.outV(), 1, false},
                {"in()", __.in(), 1, false},
                {"in(args)", __.in("test"), 1, false},
                {"both()", __.both(), 1, false},
                {"both(args)", __.both("test"), 1, false},
                {"out()", __.out(), 1, false},
                {"out(args)", __.out("test"), 1, false},
                {"out().inE().otherV", __.out().inE().otherV(), 3, false},
                {"addV()", traversalWithAddV, 2, true},
                {"addInE()", __.addInE("test", "x"), 1, true},
                {"addOutE()", __.addOutE("test", "x"), 1, true},
                {"addInE()", __.addInE("test", "x", "other", "args"), 1, true},
                {"addOutE()", __.addOutE("test", "x", "other", "args"), 1, true},
                {"addBothE(OUT)", __.addE(Direction.OUT, "test", "x"), 1, true},
                {"addBothE(IN)", __.addE(Direction.IN, "test", "x"), 1, true},
                {"in().out()", __.in().out(), 2, false},
                {"out().out().out()", __.out().out().out(), 3, false},
                {"in().out().in()", __.in().out().in(), 3, false},
                {"inE().outV().inE().outV()", __.inE().outV().inE().outV(), 4, false}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Traversal traversal;

    @Parameterized.Parameter(value = 2)
    public int expectedInsertedSteps;

    @Parameterized.Parameter(value = 3)
    public boolean hasMutatingStep;

    @Test
    public void shouldIncludeAdditionalHasStepsAndAppendPartitionOnMutatingSteps() {
        final PartitionStrategy strategy = new PartitionStrategy("p", "a");

        if (hasMutatingStep) {
            if (TraversalHelper.hasStepOfAssignableClass(AddEdgeStep.class, traversal.asAdmin())) {
                final Direction d = TraversalHelper.getStepsOfClass(AddEdgeStep.class, traversal.asAdmin()).get(0).getDirection();
                strategy.apply(traversal.asAdmin());

                final List<AddEdgeStep> addEdgeSteps = TraversalHelper.getStepsOfAssignableClass(AddEdgeStep.class, traversal.asAdmin());
                assertEquals(1, addEdgeSteps.size());

                addEdgeSteps.forEach(s -> {
                    final Object[] keyValues = s.getKeyValues();
                    final List<Pair<String, Object>> pairs = ElementHelper.asPairs(keyValues);
                    assertEquals("test", s.getEdgeLabel());
                    assertEquals(d, s.getDirection());
                    assertTrue(pairs.stream().anyMatch(p -> p.getValue0().equals("p") && p.getValue1().equals("a")));
                });
            } else if (TraversalHelper.hasStepOfAssignableClass(AddVertexStep.class, traversal.asAdmin())) {
                strategy.apply(traversal.asAdmin());

                final List<AddVertexStep> addVertexSteps = TraversalHelper.getStepsOfAssignableClass(AddVertexStep.class, traversal.asAdmin());
                assertEquals(1, addVertexSteps.size());

                addVertexSteps.forEach(s -> {
                    final Object[] keyValues = s.getKeyValues();
                    final List<Pair<String, Object>> pairs = ElementHelper.asPairs(keyValues);
                    assertTrue(pairs.stream().anyMatch(p -> p.getValue0().equals("p") && p.getValue1().equals("a")));
                });
            } else
                fail("This test should not be marked as having a mutating step or there is something else amiss.");
        } else {
            strategy.apply(traversal.asAdmin());
        }

        final List<HasStep> steps = TraversalHelper.getStepsOfClass(HasStep.class, traversal.asAdmin());
        assertEquals(expectedInsertedSteps, steps.size());

        final List<String> keySet = new ArrayList<>(strategy.getReadPartitions());
        steps.forEach(s -> {
            assertEquals(1, s.getHasContainers().size());
            final HasContainer hasContainer = (HasContainer) s.getHasContainers().get(0);
            assertEquals("p", hasContainer.key);
            assertEquals(keySet, hasContainer.value);
            assertEquals(Contains.within, hasContainer.predicate);
        });
    }
}
