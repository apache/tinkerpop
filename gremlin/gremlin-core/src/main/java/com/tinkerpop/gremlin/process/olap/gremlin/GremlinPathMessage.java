package com.tinkerpop.gremlin.process.olap.gremlin;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.util.MapHelper;
import com.tinkerpop.gremlin.process.oltp.util.SingleIterator;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.process.olap.MessageType;
import com.tinkerpop.gremlin.process.olap.Messenger;
import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.MicroPath;
import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.util.GremlinHelper;

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
                                  final Traversal gremlin) {


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
                    final Step step = GremlinHelper.getAs(holder.getFuture(), gremlin);
                    step.addStarts(new SingleIterator(holder));
                    if (processStep(step, vertex, messenger, tracker))
                        voteToHalt.set(false);
                }
            });
        });
        return voteToHalt.get();

    }

    private boolean executePaths(final Vertex vertex, final Messenger messenger,
                                 final GremlinPaths tracker,
                                 final Traversal gremlin) {
        if (this.holder.isDone()) {
            this.holder.deflate();
            MapHelper.incr(tracker.getDoneGraphTracks(), this.holder.get(), this.holder);
            return false;
        }

        final Step step = GremlinHelper.getAs(this.holder.getFuture(), gremlin);
        MapHelper.incr(tracker.getGraphTracks(), this.holder.get(), this.holder);
        step.addStarts(new SingleIterator(this.holder));
        return processStep(step, vertex, messenger, tracker);
    }

    private static boolean processStep(final Step<?, ?> step, final Vertex vertex, final Messenger messenger, final GremlinPaths tracker) {
        final boolean messageSent = step.hasNext();
        step.forEachRemaining(holder -> {
            final Object end = holder.get();
            if (end instanceof Element || end instanceof Property) {
                messenger.sendMessage(
                        vertex,
                        MessageType.Global.of(GremlinVertexProgram.GREMLIN_MESSAGE, getHostingVertices(end)),
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
