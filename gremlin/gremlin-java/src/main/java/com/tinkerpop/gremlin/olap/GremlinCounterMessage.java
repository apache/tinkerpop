package com.tinkerpop.gremlin.olap;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.MapHelper;
import com.tinkerpop.gremlin.util.SingleIterator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinCounterMessage extends GremlinMessage {

    private Long counter;

    private GremlinCounterMessage(final Holder holder) {
        super(holder);
        this.counter = 1l;
    }

    public static GremlinCounterMessage of(final Holder holder) {
        return new GremlinCounterMessage(holder);
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

        messages.forEach(message -> {
            message.holder.inflate(vertex);
            if (message.executeCounts(tracker, gremlin, localCounts))
                voteToHalt.set(false);
        });

        tracker.getPreviousObjectTracks().forEach((holder, counts) -> {
            if (holder.isDone()) {
                MapHelper.incr(tracker.getDoneObjectTracks(), holder, counts);
            } else {
                final Pipe pipe = GremlinHelper.getAs(holder.getFuture(), gremlin);
                for (int i = 0; i < counts; i++) {
                    pipe.addStarts(new SingleIterator(holder));
                }
                if (processPipe(pipe, localCounts))
                    voteToHalt.set(false);
            }
        });

        localCounts.forEach((holder, count) -> {
            if (holder.get() instanceof Element || holder.get() instanceof Property) {
                final Object end = holder.get();
                final GremlinCounterMessage message = GremlinCounterMessage.of(holder);
                message.setCounter(count);
                messenger.sendMessage(
                        vertex,
                        MessageType.Global.of(GremlinVertexProgram.GREMLIN_MESSAGE, GremlinMessage.getHostingVertices(end)),
                        message);
            } else {
                MapHelper.incr(tracker.getObjectTracks(), holder, count);
            }
        });
        return voteToHalt.get();
    }

    private boolean executeCounts(final GremlinCounters tracker,
                                  final Gremlin gremlin, Map<Holder, Long> localCounts) {

        if (this.holder.isDone()) {
            this.holder.deflate();
            MapHelper.incr(tracker.getDoneGraphTracks(), this.holder, this.counter);
            return false;
        }

        final Pipe pipe = GremlinHelper.getAs(this.holder.getFuture(), gremlin);


        MapHelper.incr(tracker.getGraphTracks(), this.holder, this.counter);
        for (int i = 0; i < this.counter; i++) {
            pipe.addStarts(new SingleIterator(this.holder));
        }
        return processPipe(pipe, localCounts);
    }

    private static boolean processPipe(final Pipe<?, ?> pipe, final Map<Holder, Long> localCounts) {
        final boolean messageSent = pipe.hasNext();
        pipe.forEachRemaining(holder -> MapHelper.incr(localCounts, holder, 1l));
        return messageSent;
    }
}


