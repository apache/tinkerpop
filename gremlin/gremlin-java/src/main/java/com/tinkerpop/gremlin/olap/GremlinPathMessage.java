package com.tinkerpop.gremlin.olap;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.gremlin.GremlinJ;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.MicroPath;
import com.tinkerpop.gremlin.Path;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.util.GremlinHelper;
import com.tinkerpop.gremlin.util.MapHelper;
import com.tinkerpop.gremlin.util.SingleIterator;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinPathMessage extends GremlinMessage {

    private GremlinPathMessage(final Holder holder) {
        super(holder);
        this.holder.setPath(MicroPath.deflate(this.holder.getPath()));
    }

    public static GremlinPathMessage of(final Holder holder) {
        return new GremlinPathMessage(holder);
    }

    public static boolean execute(final Vertex vertex,
                                  final Iterable<GremlinPathMessage> messages,
                                  final Messenger messenger,
                                  final GremlinPaths tracker,
                                  final GremlinJ gremlin) {


        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        messages.forEach(message -> {
            message.holder.inflate(vertex);
            if (message.executePaths(vertex, messenger, tracker, gremlin))
                voteToHalt.set(false);
        });
        tracker.getPreviousObjectTracks().forEach((object, holders) -> {
            holders.forEach(holder -> {
                if (holder.isDone()) {
                    if (object instanceof Path) {
                        MapHelper.incr(tracker.getDoneObjectTracks(), MicroPath.deflate(((Path) object)), holder);
                    } else {
                        MapHelper.incr(tracker.getDoneObjectTracks(), object, holder);
                    }
                } else {
                    final Pipe pipe = GremlinHelper.getAs(holder.getFuture(), gremlin);
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
                                 final GremlinJ gremlin) {
        if (this.holder.isDone()) {
            this.holder.deflate();
            MapHelper.incr(tracker.getDoneGraphTracks(), this.holder.get(), this.holder);
            return false;
        }

        final Pipe pipe = GremlinHelper.getAs(this.holder.getFuture(), gremlin);
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
                        MessageType.Global.of(GremlinVertexProgram.GREMLIN_MESSAGE, GremlinMessage.getHostingVertices(end)),
                        GremlinPathMessage.of(holder));
            } else {
                if (end instanceof Path) {
                    MapHelper.incr(tracker.getObjectTracks(), MicroPath.deflate(((Path) end)), holder);
                } else {
                    MapHelper.incr(tracker.getObjectTracks(), end, holder);
                }
            }
        });
        return messageSent;
    }

}
