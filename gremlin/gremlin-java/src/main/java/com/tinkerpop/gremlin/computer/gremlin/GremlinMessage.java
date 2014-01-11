package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.MessageType;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinMessage implements Serializable {

    public enum Destination {

        VERTEX, EDGE, PROPERTY;

        public static Destination of(Object object) {
            if (object instanceof Vertex)
                return VERTEX;
            else if (object instanceof Edge)
                return EDGE;
            else if (object instanceof Property)
                return PROPERTY;
            else
                throw new IllegalArgumentException("Unknown type: " + object.getClass());
        }
    }

    public final Object elementId;
    public final Destination destination;
    public final String propertyKey;
    public Holder holder;
    public Long counter = 1l;

    private GremlinMessage(final Destination destination, final Object elementId, final String propertyKey, final Holder holder) {
        this.destination = destination;
        this.elementId = elementId;
        this.propertyKey = propertyKey;
        this.holder = holder;
        this.holder.getPath().microSize();
    }

    public static GremlinMessage of(final Holder holder) {
        final Destination destination = Destination.of(holder.get());
        if (destination == Destination.VERTEX)
            return new GremlinMessage(destination, ((Vertex) holder.get()).getId(), null, holder);
        else if (destination == Destination.EDGE)
            return new GremlinMessage(destination, ((Edge) holder.get()).getId(), null, holder);
        else
            return new GremlinMessage(destination, ((Property) holder.get()).getElement().getId(), ((Property) holder.get()).getKey(), holder);

    }

    public void setCounter(final Long counter) {
        this.counter = counter;
    }

    public Holder getHolder() {
        return this.holder;
    }

    public static boolean executePaths(final Vertex vertex,
                                       final Iterable<GremlinMessage> messages,
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
                    final Pipe<?, ?> pipe = PipelineHelper.getAs(holder.getFuture(), gremlin);
                    pipe.addStarts(new SingleIterator(holder));
                    if (processPipe(pipe, vertex, messenger, tracker))
                        voteToHalt.set(false);
                }
            });
        });
        return voteToHalt.get();

    }

    public static boolean executeCounts(final Vertex vertex,
                                        final Iterable<GremlinMessage> messages,
                                        final Messenger messenger,
                                        final GremlinCounter tracker,
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
                final Pipe<?, ?> pipe = PipelineHelper.getAs(holder.getFuture(), gremlin);
                for (int i = 0; i < counts; i++) {
                    pipe.addStarts(new SingleIterator(holder));
                }
                if (processPipe(pipe, localCounts))
                    voteToHalt.set(false);
            }
        });

        localCounts.forEach((o, c) -> {
            if (o.get() instanceof Element || o.get() instanceof Property) {
                final GremlinMessage message = GremlinMessage.of(o);
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
                                  final GremlinCounter tracker,
                                  final Gremlin gremlin, Map<Holder, Long> localCounts) {
        if (this.holder.isDone()) {
            MapHelper.incr(tracker.getDoneGraphTracks(), this.holder, this.counter);
            return false;
        }

        final Pipe<?, ?> pipe = PipelineHelper.getAs(this.holder.getFuture(), gremlin);
        if (!this.stageHolder(vertex))
            return false;

        MapHelper.incr(tracker.getGraphTracks(), this.holder, this.counter);
        for (int i = 0; i < this.counter; i++) {
            pipe.addStarts(new SingleIterator(this.holder));
        }
        return processPipe(pipe, localCounts);
    }

    private boolean executePaths(final Vertex vertex, final Messenger messenger,
                                 final GremlinPaths tracker,
                                 final Gremlin gremlin) {
        if (this.holder.isDone()) {
            MapHelper.incr(tracker.getDoneGraphTracks(), this.holder.get(), this.holder);
            return false;
        }

        final Pipe<?, ?> pipe = PipelineHelper.getAs(this.holder.getFuture(), gremlin);

        if (!this.stageHolder(vertex))
            return false;
        MapHelper.incr(tracker.getGraphTracks(), this.holder.get(), this.holder);
        pipe.addStarts(new SingleIterator(this.holder));
        return processPipe(pipe, vertex, messenger, tracker);
    }

    ///////////////////////////////////

    private boolean stageHolder(final Vertex vertex) {
        if (this.destination.equals(Destination.VERTEX)) {
            this.holder.set(vertex);
        } else if (this.destination.equals(Destination.EDGE)) {
            final Optional<Edge> edgeOptional = this.getEdge(vertex);
            if (edgeOptional.isPresent())
                this.holder.set(edgeOptional.get());
            else
                return false;
            //throw new IllegalStateException("The local edge is not present: " + this.elementId);
        } else if (this.getProperty(vertex).isPresent()) {
            final Optional<Property> propertyOptional = this.getProperty(vertex);
            if (propertyOptional.isPresent())
                this.holder.set(propertyOptional.get());
            else
                return false;
            //throw new IllegalStateException("The local property is not present: " + this.elementId + ":" + this.propertyKey);
        } else
            return false;

        return true;
    }

    private Optional<Edge> getEdge(final Vertex vertex) {
        // TODO: WHY IS THIS NOT LIKE FAUNUS WITH A BOTH?
        // TODO: I KNOW WHY -- CAUSE OF HOSTING VERTICES IS BOTH IN/OUT WHICH IS NECESSARY FOR EDGE MUTATIONS
        return StreamFactory.stream(vertex.query().direction(Direction.OUT).edges())
                .filter(e -> e.getId().equals(this.elementId))
                .findFirst();
    }

    private Optional<Property> getProperty(final Vertex vertex) {
        if (this.elementId.equals(vertex.getId())) {
            final Property property = vertex.getProperty(this.propertyKey);
            return property.isPresent() ? Optional.of(property) : Optional.empty();
        } else {
            return (Optional) StreamFactory.stream(vertex.query().direction(Direction.OUT).edges())
                    .filter(e -> e.getId().equals(this.elementId))
                    .map(e -> e.getProperty(this.propertyKey))
                    .findFirst();
        }
    }

    protected static boolean processPipe(final Pipe<?, ?> pipe, final Map<Holder, Long> localCounts) {
        final boolean messageSent = pipe.hasNext();
        pipe.forEachRemaining(h -> {
            MapHelper.incr(localCounts, h, 1l);
        });
        return messageSent;
    }

    protected static boolean processPipe(final Pipe<?, ?> pipe, final Vertex vertex, final Messenger messenger, final GremlinPaths tracker) {
        final boolean messageSent = pipe.hasNext();
        pipe.forEachRemaining(holder -> {
            final Object end = holder.get();
            if (end instanceof Element || end instanceof Property) {
                messenger.sendMessage(
                        vertex,
                        MessageType.Global.of(GremlinVertexProgram.GREMLIN_MESSAGE, Messenger.getHostingVertices(end)),
                        GremlinMessage.of(holder));
            } else {
                MapHelper.incr(tracker.getObjectTracks(), end, holder);
            }
        });
        return messageSent;
    }
}