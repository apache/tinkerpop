package com.tinkerpop.gremlin.process.olap.traversal;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.olap.GraphMemory;
import com.tinkerpop.gremlin.process.olap.MessageType;
import com.tinkerpop.gremlin.process.olap.Messenger;
import com.tinkerpop.gremlin.process.olap.VertexProgram;
import com.tinkerpop.gremlin.process.steps.map.EdgesStep;
import com.tinkerpop.gremlin.process.steps.map.VerticesStep;
import com.tinkerpop.gremlin.process.steps.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalVertexProgram<M extends TraversalMessage> implements VertexProgram<M> {

    private MessageType.Global global = MessageType.Global.of(GREMLIN_MESSAGE);

    protected static final String GREMLIN_MESSAGE = "gremlinMessage";
    private static final String GREMLIN_TRAVERSAL = "gremlinTraversal";
    private static final String VOTE_TO_HALT = "voteToHalt";
    public static final String TRACK_PATHS = "trackPaths";
    // TODO: public static final String MESSAGES_SENT = "messagesSent";
    public static final String GREMLIN_TRACKER = "gremlinTracker";
    private final Supplier<Traversal> gremlinSupplier;

    private TraversalVertexProgram(final Supplier<Traversal> gremlinSupplier) {
        this.gremlinSupplier = gremlinSupplier;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent(GREMLIN_TRAVERSAL, this.gremlinSupplier);
        graphMemory.setIfAbsent(VOTE_TO_HALT, true);
        graphMemory.setIfAbsent(TRACK_PATHS, HolderOptimizer.trackPaths(this.gremlinSupplier.get()));
    }

    public void execute(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        if (graphMemory.isInitialIteration()) {
            executeFirstIteration(vertex, messenger, graphMemory);
        } else {
            executeOtherIterations(vertex, messenger, graphMemory);
        }
    }

    private void executeFirstIteration(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        final Traversal gremlin = graphMemory.<Supplier<Traversal>>get(GREMLIN_TRAVERSAL).get();
        if (null != graphMemory.getReductionMemory())
            gremlin.addStep(new ReductionStep(gremlin, graphMemory.getReductionMemory()));
        // the head is always an IdentityStep so simply skip it
        final Step startStep = (VerticesStep) gremlin.getSteps().get(1);
        final String future = (gremlin.getSteps().size() == 2) ? Holder.NO_FUTURE : ((Step) gremlin.getSteps().get(2)).getAs();

        // TODO: Was doing some HasContainer.testAll() stuff prior to the big change (necessary?)
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        if (startStep instanceof VerticesStep) {
            final Holder<Vertex> holder = graphMemory.<Boolean>get(TRACK_PATHS) ?
                    new PathHolder<>(startStep.getAs(), vertex) :
                    new SimpleHolder<>(vertex);
            holder.setFuture(future);
            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), TraversalMessage.of(holder));
            voteToHalt.set(false);
        } else if (startStep instanceof EdgesStep) {
            vertex.outE().forEach(e -> {
                        final Holder<Edge> holder = graphMemory.<Boolean>get(TRACK_PATHS) ?
                                new PathHolder<>(startStep.getAs(), e) :
                                new SimpleHolder<>(e);
                        holder.setFuture(future);
                        messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), TraversalMessage.of(holder));
                        voteToHalt.set(false);
                    });
        }
        graphMemory.and(VOTE_TO_HALT, voteToHalt.get());
    }

    private void executeOtherIterations(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        final Traversal gremlin = graphMemory.<Supplier<Traversal>>get(GREMLIN_TRAVERSAL).get();
        if (null != graphMemory.getReductionMemory())
            gremlin.addStep(new ReductionStep(gremlin, graphMemory.getReductionMemory()));
        if (graphMemory.<Boolean>get(TRACK_PATHS)) {
            final TraversalPaths tracker = new TraversalPaths(vertex);
            graphMemory.and(VOTE_TO_HALT, TraversalPathMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, gremlin));
            vertex.setProperty(GREMLIN_TRACKER, tracker);
        } else {
            final TraversalCounters tracker = new TraversalCounters(vertex);
            graphMemory.and(VOTE_TO_HALT, TraversalCounterMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, gremlin));
            vertex.setProperty(GREMLIN_TRACKER, tracker);
        }
    }

    ////////// GRAPH COMPUTER METHODS

    public boolean terminate(final GraphMemory graphMemory) {
        final boolean voteToHalt = graphMemory.get(VOTE_TO_HALT);
        if (voteToHalt) {
            return true;
        } else {
            graphMemory.or(VOTE_TO_HALT, true);
            return false;
        }
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(GREMLIN_TRACKER, KeyType.VARIABLE);
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private Supplier<Traversal> gremlin;

        public Builder gremlin(final Supplier<Traversal> gremlin) {
            this.gremlin = gremlin;
            return this;
        }

        public TraversalVertexProgram build() {
            return new TraversalVertexProgram(this.gremlin);
        }
    }
}