package com.tinkerpop.gremlin.olap;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.MapHelper;
import com.tinkerpop.gremlin.util.SingleIterator;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinPathMessage extends GremlinMessage {

    public GremlinPathMessage(final Destination destination, final Object elementId, final String propertyKey, final Holder holder) {
        super(destination, elementId, propertyKey, holder);
    }

    public static GremlinPathMessage of(final Holder holder) {
        final Destination destination = Destination.of(holder.get());
        if (destination == Destination.VERTEX)
            return new GremlinPathMessage(destination, ((Vertex) holder.get()).getId(), null, holder);
        else if (destination == Destination.EDGE)
            return new GremlinPathMessage(destination, ((Edge) holder.get()).getId(), null, holder);
        else
            return new GremlinPathMessage(destination, ((Property) holder.get()).getElement().getId(), ((Property) holder.get()).getKey(), holder);

    }

    public static boolean execute(final Vertex vertex,
                                  final Iterable<GremlinPathMessage> messages,
                                  final Messenger messenger,
                                  final GremlinPaths tracker,
                                  final Gremlin gremlin) {


        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        messages.forEach(m -> {
            if (m.executePaths(vertex, messenger, tracker, gremlin))
                voteToHalt.set(false);
        });
        tracker.getPreviousObjectTracks().forEach((a, b) -> {
            b.forEach(holder -> {
                if (holder.isDone()) {
                    MapHelper.incr(tracker.getDoneObjectTracks(), a, holder);
                } else {
                    final Pipe<?, ?> pipe = GremlinHelper.getAs(holder.getFuture(), gremlin);
                    pipe.addStarts(new SingleIterator(holder));
                    if (processPipe(pipe, vertex, messenger, tracker))
                        voteToHalt.set(false);
                }
            });
        });
        return voteToHalt.get();

    }

    private boolean executePaths(final Vertex vertex, final Messenger messenger,
                                 final GremlinPaths tracker,
                                 final Gremlin gremlin) {
        if (this.holder.isDone()) {
            MapHelper.incr(tracker.getDoneGraphTracks(), this.holder.get(), this.holder);
            return false;
        }

        final Pipe<?, ?> pipe = GremlinHelper.getAs(this.holder.getFuture(), gremlin);

        if (!this.stageHolder(vertex))
            return false;
        MapHelper.incr(tracker.getGraphTracks(), this.holder.get(), this.holder);
        pipe.addStarts(new SingleIterator(this.holder));
        return processPipe(pipe, vertex, messenger, tracker);
    }

    private static boolean processPipe(final Pipe<?, ?> pipe, final Vertex vertex, final Messenger messenger, final GremlinPaths tracker) {
        final boolean messageSent = pipe.hasNext();
        pipe.forEachRemaining(holder -> {
            final Object end = holder.get();
            if (end instanceof Element || end instanceof Property) {
                messenger.sendMessage(
                        vertex,
                        MessageType.Global.of(GremlinVertexProgram.GREMLIN_MESSAGE, Messenger.getHostingVertices(end)),
                        GremlinPathMessage.of(holder));
            } else {
                MapHelper.incr(tracker.getObjectTracks(), end, holder);
            }
        });
        return messageSent;
    }

}
