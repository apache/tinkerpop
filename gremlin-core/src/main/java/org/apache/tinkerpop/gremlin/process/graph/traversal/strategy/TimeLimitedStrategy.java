package org.apache.tinkerpop.gremlin.process.graph.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.filter.TimeLimitStep;

/**
 * A {@link org.apache.tinkerpop.gremlin.process.TraversalStrategy} that prevents a traversal from running beyond
 * a pre-configured time.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TimeLimitedStrategy extends AbstractTraversalStrategy {
    private final long timeLimit;

    public TimeLimitedStrategy(final long timeLimit) {
        this.timeLimit = timeLimit;
    }

    public long getTimeLimit() {
        return timeLimit;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getEndStep().getClass().equals(TimeLimitStep.class))
            return;

        traversal.addStep(new TimeLimitStep<>(traversal, timeLimit));
    }
}
