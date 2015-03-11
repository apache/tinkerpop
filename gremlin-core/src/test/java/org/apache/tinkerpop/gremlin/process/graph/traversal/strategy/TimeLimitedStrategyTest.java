package org.apache.tinkerpop.gremlin.process.graph.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.graph.traversal.__;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.TimeLimitStep;
import org.apache.tinkerpop.gremlin.process.traversal.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TimeLimitedStrategyTest {

    @Test
    public void shouldAddTimeLimitStepIfNotPresent() {
        final Traversal t = __.out();

        final TimeLimitedStrategy s = new TimeLimitedStrategy(100);
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(s);
        t.asAdmin().setStrategies(strategies);

        t.asAdmin().applyStrategies();

        assertEquals(TimeLimitStep.class, t.asAdmin().getEndStep().getClass());
    }

    @Test
    public void shouldNotAddTimeLimitStepIfAlreadyPresentAtEnd() {
        final Traversal t = __.out().timeLimit(100);

        final TimeLimitedStrategy s = new TimeLimitedStrategy(100);
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(s);
        t.asAdmin().setStrategies(strategies);

        t.asAdmin().applyStrategies();

        assertEquals(1, TraversalHelper.getStepsOfAssignableClass(TimeLimitStep.class, t.asAdmin()).stream().count());
        assertEquals(TimeLimitStep.class, t.asAdmin().getEndStep().getClass());
    }

    @Test
    public void shouldAddTimeLimitStepIfAlreadyPresentInMiddle() {
        final Traversal t = __.out().timeLimit(100).out();

        final TimeLimitedStrategy s = new TimeLimitedStrategy(100);
        final TraversalStrategies strategies = new DefaultTraversalStrategies();
        strategies.addStrategies(s);
        t.asAdmin().setStrategies(strategies);

        t.asAdmin().applyStrategies();

        assertEquals(TimeLimitStep.class, t.asAdmin().getEndStep().getClass());
    }
}
