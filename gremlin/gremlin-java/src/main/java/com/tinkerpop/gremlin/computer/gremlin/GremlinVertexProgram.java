package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphMemory;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.computer.VertexProgram;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinVertexProgram implements VertexProgram<GremlinMessage> {

    private MessageType.Global global = MessageType.Global.of(GREMLIN_MESSAGE);

    protected static final String GREMLIN_MESSAGE = "gremlinMessage";
    private static final String GREMLIN_PIPELINE = "gremlinPipeline";
    private static final String VOTE_TO_HALT = "voteToHalt";

    public static final String GREMLIN_TRACKER = "gremlinTracker";
    private final Supplier<Gremlin> gremlin;

    private GremlinVertexProgram(final Supplier<Gremlin> gremlin) {
        this.gremlin = gremlin;
    }

    public void setup(final GraphMemory graphMemory) {
        graphMemory.setIfAbsent(GREMLIN_PIPELINE, this.gremlin);
        graphMemory.setIfAbsent(VOTE_TO_HALT, true);
    }

    public void execute(final Vertex vertex, final Messenger<GremlinMessage> messenger, final GraphMemory graphMemory) {
        final Gremlin gremlin = graphMemory.<Supplier<Gremlin>>get(GREMLIN_PIPELINE).get();

        if (graphMemory.isInitialIteration()) {
            final Holder holder = new Holder<>(Holder.NONE, vertex);
            holder.setFuture(PipelineHelper.getStart(gremlin).getAs());
            messenger.sendMessage(vertex, MessageType.Global.of(GREMLIN_MESSAGE, vertex), GremlinMessage.of(vertex, holder));
            graphMemory.and(VOTE_TO_HALT, false);
        } else {
            final GremlinTracker tracker = new GremlinTracker(vertex);

            // RECEIVE MESSAGES
            final AtomicBoolean voteToHalt = new AtomicBoolean(true);
            messenger.receiveMessages(vertex, this.global).forEach(m -> {
                if (m.execute(vertex, messenger, tracker, gremlin))
                    voteToHalt.set(false);
            });
            // process local object messages
            tracker.getPreviousObjectHolders().forEach((a, b) -> {
                b.forEach(holder -> {
                    if (holder.isDone()) {
                        MapHelper.incr(tracker.getDoneObjectHolders(), a, holder);
                    } else {
                        final Pipe<?, ?> pipe = PipelineHelper.getAs(holder.getFuture(), gremlin);
                        pipe.addStarts(new SingleIterator(holder));
                        if (processPipe(pipe, vertex, messenger, tracker))
                            voteToHalt.set(false);
                    }
                });
            });

            // SET NEW TRACKER AND VOTE TO HALT
            vertex.setProperty(GREMLIN_TRACKER, tracker);
            graphMemory.and(VOTE_TO_HALT, voteToHalt.get());
        }
    }

    protected static boolean processPipe(final Pipe<?, ?> pipe, final Vertex vertex, final Messenger messenger, final GremlinTracker tracker) {
        final boolean messageSent = pipe.hasNext();
        pipe.forEachRemaining(h -> {
            final Object end = h.get();
            if (end instanceof Element || end instanceof Property) {
                // TODO: (OPTIMIZATION) IF THE ELEMENT IS ADJACENT USE LOCAL MESSAGE (!WAIT! WITH PATHS-ENABLED THIS WON'T WORK!)
                // TODO: (OPTIMIZATION) IF THE ELEMENT IS INCIDENT USE OBJECT COUNTERS MAP
                messenger.sendMessage(
                        vertex,
                        MessageType.Global.of(GremlinVertexProgram.GREMLIN_MESSAGE, Messenger.getHostingVertices(end)),
                        GremlinMessage.of(end, h));
            } else {
                MapHelper.incr(tracker.getObjectHolders(), end, h);
            }
        });
        return messageSent;
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