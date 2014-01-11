package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;

import java.util.Map;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram implements VertexProgram<GremlinMessage> {

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
        graphMemory.setIfAbsent(TRACK_PATHS, true);
    }

    public void execute(final Vertex vertex, final Messenger<GremlinMessage> messenger, final GraphMemory graphMemory) {
        final Gremlin gremlin = graphMemory.<Supplier<Gremlin>>get(GREMLIN_PIPELINE).get();
        if (graphMemory.isInitialIteration()) {
            final Holder holder = new Holder<>(Holder.NONE, vertex);
            holder.setFuture(PipelineHelper.getStart(gremlin).getAs());
            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), GremlinMessage.of(holder));
            graphMemory.and(VOTE_TO_HALT, false);
        } else {
            if (graphMemory.<Boolean>get(TRACK_PATHS)) {
                final GremlinPaths tracker = new GremlinPaths(vertex);
                graphMemory.and(VOTE_TO_HALT, GremlinMessage.executePaths(vertex, messenger.receiveMessages(vertex, this.global), messenger, tracker, gremlin));
                vertex.setProperty(GREMLIN_TRACKER, tracker);
            } else {
                final GremlinCounter tracker = new GremlinCounter(vertex);
                graphMemory.and(VOTE_TO_HALT, GremlinMessage.executeCounts(vertex, messenger.receiveMessages(vertex, this.global), messenger, tracker, gremlin));
                vertex.setProperty(GREMLIN_TRACKER, tracker);
            }
        }
    }

    ////////// GRAPH COMPUTER METHODS

    public boolean terminate(final GraphMemory graphMemory) {
        final boolean voteToHalt = graphMemory.get(VOTE_TO_HALT);
        // System.out.println(voteToHalt + "--" + graphMemory.getIteration());
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