package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.computer.MessageCombiner;
import com.tinkerpop.gremlin.process.util.tool.TraverserSet;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalVertexProgramMessageCombiner implements MessageCombiner<TraverserSet<?>> {

    private static final Optional<TraversalVertexProgramMessageCombiner> INSTANCE = Optional.of(new TraversalVertexProgramMessageCombiner());

    private TraversalVertexProgramMessageCombiner() {

    }

    public TraverserSet<?> combine(final TraverserSet<?> messageA, final TraverserSet<?> messageB) {
        messageA.addAll((TraverserSet) messageB);
        return messageA;
    }

    public static Optional<TraversalVertexProgramMessageCombiner> instance() {
        return INSTANCE;
    }
}
