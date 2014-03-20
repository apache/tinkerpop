package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MessageType;
import com.tinkerpop.gremlin.process.computer.Messenger;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.process.graph.map.GraphStep;
import com.tinkerpop.gremlin.process.util.HolderOptimizer;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SSupplier;
import org.apache.commons.configuration.Configuration;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalVertexProgram<M extends TraversalMessage> implements VertexProgram<M> {

    private MessageType.Global global = MessageType.Global.of();

    private static final String TRAVERSAL = "traversal";
    private static final String VOTE_TO_HALT = "voteToHalt";
    public static final String TRACK_PATHS = "trackPaths";
    // TODO: public static final String MESSAGES_SENT = "messagesSent";
    public static final String TRAVERSAL_TRACKER = "traversalTracker";
    private final SSupplier<Traversal> traversalSupplier;

    private TraversalVertexProgram(final SSupplier<Traversal> traversalSupplier) {
        this.traversalSupplier = traversalSupplier;
    }

    public void setup(final Configuration configuration, final Graph.Memory.Computer graphMemory) {
        graphMemory.setIfAbsent(TRAVERSAL, this.traversalSupplier);
        graphMemory.setIfAbsent(VOTE_TO_HALT, true);
        graphMemory.setIfAbsent(TRACK_PATHS, HolderOptimizer.trackPaths(this.traversalSupplier.get()));
    }

    public void execute(final Vertex vertex, final Messenger<M> messenger, final Graph.Memory.Computer graphMemory) {
        if (graphMemory.isInitialIteration()) {
            executeFirstIteration(vertex, messenger, graphMemory);
        } else {
            executeOtherIterations(vertex, messenger, graphMemory);
        }
    }

    private void executeFirstIteration(final Vertex vertex, final Messenger<M> messenger, final Graph.Memory.Computer graphMemory) {
        final Traversal traversal = graphMemory.<SSupplier<Traversal>>get(TRAVERSAL).get();
        final GraphStep startStep = (GraphStep) traversal.getSteps().get(0);   // TODO: make this generic to Traversal
        startStep.clear();
        final String future = (traversal.getSteps().size() == 1) ? Holder.NO_FUTURE : ((Step) traversal.getSteps().get(1)).getAs();

        // TODO: Was doing some HasContainer.testAll() stuff prior to the big change (necessary?)
        // TODO: Make this an optimizer.
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        if (Vertex.class.isAssignableFrom(startStep.returnClass)) {
            final Holder<Vertex> holder = graphMemory.<Boolean>get(TRACK_PATHS) ?
                    new PathHolder<>(startStep.getAs(), vertex) :
                    new SimpleHolder<>(vertex);
            holder.setFuture(future);
            messenger.sendMessage(vertex, MessageType.Global.of(vertex), TraversalMessage.of(holder));
            voteToHalt.set(false);
        } else if (Edge.class.isAssignableFrom(startStep.returnClass)) {
            vertex.outE().forEach(e -> {
                final Holder<Edge> holder = graphMemory.<Boolean>get(TRACK_PATHS) ?
                        new PathHolder<>(startStep.getAs(), e) :
                        new SimpleHolder<>(e);
                holder.setFuture(future);
                messenger.sendMessage(vertex, MessageType.Global.of(vertex), TraversalMessage.of(holder));
                voteToHalt.set(false);
            });
        }
        graphMemory.and(VOTE_TO_HALT, voteToHalt.get());
    }

    private void executeOtherIterations(final Vertex vertex, final Messenger<M> messenger, final Graph.Memory.Computer graphMemory) {
        final Traversal traversal = graphMemory.<SSupplier<Traversal>>get(TRAVERSAL).get();
        ((GraphStep) traversal.getSteps().get(0)).clear();
        if (graphMemory.<Boolean>get(TRACK_PATHS)) {
            final TraversalPaths tracker = new TraversalPaths(vertex);
            graphMemory.and(VOTE_TO_HALT, TraversalPathMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, traversal));
            vertex.setProperty(TRAVERSAL_TRACKER, tracker);
        } else {
            final TraversalCounters tracker = new TraversalCounters(vertex);
            graphMemory.and(VOTE_TO_HALT, TraversalCounterMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, traversal));
            vertex.setProperty(TRAVERSAL_TRACKER, tracker);
        }
    }

    ////////// GRAPH COMPUTER METHODS

    public boolean terminate(final Graph.Memory.Computer graphMemory) {
        final boolean voteToHalt = graphMemory.get(VOTE_TO_HALT);
        if (voteToHalt) {
            return true;
        } else {
            graphMemory.or(VOTE_TO_HALT, true);
            return false;
        }
    }

    public Class<M> getMessageClass() {
        return (Class) (HolderOptimizer.trackPaths(this.traversalSupplier.get()) ? TraversalPathMessage.class : TraversalCounterMessage.class);
    }

    public Map<String, KeyType> getComputeKeys() {
        return VertexProgram.ofComputeKeys(TRAVERSAL_TRACKER, KeyType.VARIABLE);
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private SSupplier<Traversal> traversalSupplier;

        public Builder traversal(final SSupplier<Traversal> traversalSupplier) {
            this.traversalSupplier = traversalSupplier;
            return this;
        }

        public TraversalVertexProgram build() {
            return new TraversalVertexProgram(this.traversalSupplier);
        }
    }
}