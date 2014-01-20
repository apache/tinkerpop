package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.blueprints.query.util.HasContainer;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.PathHolder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.SimpleHolder;
import com.tinkerpop.gremlin.pipes.map.GraphQueryPipe;
import com.tinkerpop.gremlin.pipes.util.optimizers.HolderOptimizer;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram<M extends GremlinMessage> implements VertexProgram<M> {

    private MessageType.Global global = MessageType.Global.of(GREMLIN_MESSAGE);

    protected static final String GREMLIN_MESSAGE = "gremlinMessage";
    private static final String GREMLIN_PIPELINE = "gremlinPipeline";
    private static final String VOTE_TO_HALT = "voteToHalt";
    public static final String TRACK_PATHS = "trackPaths";
    // TODO: public static final String MESSAGES_SENT = "messagesSent";
    public static final String GREMLIN_TRACKER = "gremlinTracker";
    private final Supplier<Gremlin> gremlin;

    private GremlinVertexProgram(final Supplier<Gremlin> gremlin) {
        this.gremlin = gremlin;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent(GREMLIN_PIPELINE, this.gremlin);
        graphMemory.setIfAbsent(VOTE_TO_HALT, true);
        graphMemory.setIfAbsent(TRACK_PATHS, new HolderOptimizer().trackPaths(this.gremlin.get()));
    }

    public void execute(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        if (graphMemory.isInitialIteration()) {
            executeFirstIteration(vertex, messenger, graphMemory);
        } else {
            executeOtherIterations(vertex, messenger, graphMemory);
        }
    }

    private void executeFirstIteration(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        final Gremlin gremlin = graphMemory.<Supplier<Gremlin>>get(GREMLIN_PIPELINE).get();
        final GraphQueryPipe graphQueryPipe = (GraphQueryPipe) gremlin.getPipes().get(0);
        final String future = (gremlin.getPipes().size() == 1) ? Holder.NO_FUTURE : ((Pipe) gremlin.getPipes().get(1)).getAs();

        final List<HasContainer> hasContainers = graphQueryPipe.queryBuilder.hasContainers;
        if (graphQueryPipe.returnClass.equals(Vertex.class) && HasContainer.testAll(vertex, hasContainers)) {
            final Holder<Vertex> holder = graphMemory.<Boolean>get(TRACK_PATHS) ? new PathHolder<>(graphQueryPipe.getAs(), vertex) : new SimpleHolder<>(graphQueryPipe.getAs(), vertex);
            holder.setFuture(future);
            if (graphMemory.<Boolean>get(TRACK_PATHS))
                messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), (M) GremlinPathMessage.of(holder));
            else
                messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), (M) GremlinCounterMessage.of(holder));
        } else if (graphQueryPipe.returnClass.equals(Edge.class)) {
            StreamFactory.stream(vertex.query().direction(Direction.OUT).edges())
                    .filter(e -> HasContainer.testAll(e, hasContainers))
                    .forEach(e -> {
                        final Holder<Edge> holder = graphMemory.<Boolean>get(TRACK_PATHS) ? new PathHolder<>(e) : new SimpleHolder<>(e);
                        holder.setFuture(future);
                        if (graphMemory.<Boolean>get(TRACK_PATHS))
                            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), (M) GremlinPathMessage.of(holder));
                        else
                            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), (M) GremlinCounterMessage.of(holder));
                    });
        }

        graphMemory.and(VOTE_TO_HALT, false);
    }

    private void executeOtherIterations(final Vertex vertex, final Messenger<M> messenger, final GraphMemory graphMemory) {
        final Gremlin gremlin = graphMemory.<Supplier<Gremlin>>get(GREMLIN_PIPELINE).get();
        if (graphMemory.<Boolean>get(TRACK_PATHS)) {
            final GremlinPaths tracker = new GremlinPaths(vertex);
            graphMemory.and(VOTE_TO_HALT, GremlinPathMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, gremlin));
            vertex.setProperty(GREMLIN_TRACKER, tracker);
        } else {
            final GremlinCounters tracker = new GremlinCounters(vertex);
            graphMemory.and(VOTE_TO_HALT, GremlinCounterMessage.execute(vertex, (Iterable) messenger.receiveMessages(vertex, this.global), messenger, tracker, gremlin));
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
        private Supplier<Gremlin> gremlin;

        public Builder gremlin(final Supplier<Gremlin> gremlin) {
            this.gremlin = gremlin;
            return this;
        }

        public GremlinVertexProgram build() {
            return new GremlinVertexProgram(this.gremlin);
        }
    }
}