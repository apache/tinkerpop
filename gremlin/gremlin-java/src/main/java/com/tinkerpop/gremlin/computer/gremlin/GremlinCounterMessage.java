package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinCounterMessage extends GremlinMessage {

    private Long counter;

    public GremlinCounterMessage(final Destination destination, final Object elementId, final String propertyKey, final Holder holder) {
        super(destination, elementId, propertyKey, holder);
        this.counter = 1l;
    }


    public static GremlinCounterMessage of(final Holder holder) {
        final Destination destination = Destination.of(holder.get());
        if (destination == Destination.VERTEX)
            return new GremlinCounterMessage(destination, ((Vertex) holder.get()).getId(), null, holder);
        else if (destination == Destination.EDGE)
            return new GremlinCounterMessage(destination, ((Edge) holder.get()).getId(), null, holder);
        else
            return new GremlinCounterMessage(destination, ((Property) holder.get()).getElement().getId(), ((Property) holder.get()).getKey(), holder);

    }

    public void setCounter(final Long counter) {
        this.counter = counter;
    }

    public static boolean execute(final Vertex vertex,
                                  final Iterable<GremlinCounterMessage> messages,
                                  final Messenger messenger,
                                  final GremlinCounters tracker,
                                  final Gremlin gremlin) {

        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final Map<Holder, Long> localCounts = new HashMap<>();

        messages.forEach(m -> {
            if (m.executeCounts(vertex, tracker, gremlin, localCounts))
                voteToHalt.set(false);
        });

        tracker.getPreviousObjectTracks().forEach((holder, counts) -> {
            if (holder.isDone()) {
                MapHelper.incr(tracker.getDoneObjectTracks(), holder, counts);
            } else {
                final Pipe<?, ?> pipe = GremlinHelper.getAs(holder.getFuture(), gremlin);
                for (int i = 0; i < counts; i++) {
                    pipe.addStarts(new SingleIterator(holder));
                }
                if (processPipe(pipe, localCounts))
                    voteToHalt.set(false);
            }
        });

        localCounts.forEach((o, c) -> {
            if (o.get() instanceof Element || o.get() instanceof Property) {
                final GremlinCounterMessage message = GremlinCounterMessage.of(o);
                message.setCounter(c);
                messenger.sendMessage(
                        vertex,
                        MessageType.Global.of(GremlinVertexProgram.GREMLIN_MESSAGE, Messenger.getHostingVertices(o.get())),
                        message);
            } else {
                MapHelper.incr(tracker.getObjectTracks(), o, c);
            }
        });
        return voteToHalt.get();
    }

    private boolean executeCounts(final Vertex vertex,
                                  final GremlinCounters tracker,
                                  final Gremlin gremlin, Map<Holder, Long> localCounts) {
        if (this.holder.isDone()) {
            MapHelper.incr(tracker.getDoneGraphTracks(), this.holder, this.counter);
            return false;
        }

        final Pipe<?, ?> pipe = GremlinHelper.getAs(this.holder.getFuture(), gremlin);
        if (!this.stageHolder(vertex))
            return false;

        MapHelper.incr(tracker.getGraphTracks(), this.holder, this.counter);
        for (int i = 0; i < this.counter; i++) {
            pipe.addStarts(new SingleIterator(this.holder));
        }
        return processPipe(pipe, localCounts);
    }

    private static boolean processPipe(final Pipe<?, ?> pipe, final Map<Holder, Long> localCounts) {
        final boolean messageSent = pipe.hasNext();
        pipe.forEachRemaining(h -> {
            MapHelper.incr(localCounts, h, 1l);
        });
        return messageSent;
    }
}


