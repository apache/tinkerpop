package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.util.TraverserSet;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalVertexProgramMessageCombiner implements MessageCombiner<TraverserSet<?>> {

    private Traversal traversal;
    private boolean hasMergeOperator;

    public TraversalVertexProgramMessageCombiner(final Traversal traversal) {
        this.traversal = traversal;
        this.hasMergeOperator = this.traversal.sideEffects().getSackMergeOperator().isPresent();
    }

    public TraverserSet<?> combine(final TraverserSet<?> messageA, final TraverserSet<?> messageB) {
        // necessary so the the sack merge operator is available mid-wire, on combine.
        if (this.hasMergeOperator) {
            messageA.forEach(traverser -> traverser.setSideEffects(this.traversal.sideEffects()));
            messageB.forEach(traverser -> traverser.setSideEffects(this.traversal.sideEffects()));
        }
        messageA.addAll((TraverserSet) messageB);
        return messageA;
    }
}
