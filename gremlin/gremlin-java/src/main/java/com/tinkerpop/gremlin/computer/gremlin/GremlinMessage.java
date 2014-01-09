package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.Messenger;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.Gremlin;
import com.tinkerpop.gremlin.pipes.Pipe;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.MapHelper;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;
import com.tinkerpop.gremlin.pipes.util.SingleIterator;

import java.io.Serializable;
import java.util.Optional;

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

    private GremlinMessage(final Destination destination, final Object elementId, final String propertyKey, final Holder holder) {
        this.destination = destination;
        this.elementId = elementId;
        this.propertyKey = propertyKey;
        this.holder = holder;
        this.holder.getPath().microSize();
    }

    public static GremlinMessage of(final Object object, final Holder holder) {
        final Destination destination = Destination.of(object);
        if (destination == Destination.VERTEX)
            return new GremlinMessage(destination, ((Vertex) object).getId(), null, holder);
        else if (destination == Destination.EDGE)
            return new GremlinMessage(destination, ((Edge) object).getId(), null, holder);
        else
            return new GremlinMessage(destination, ((Property) object).getElement().getId(), ((Property) object).getKey(), holder);

    }

    public Holder getHolder() {
        return this.holder;
    }

    public boolean execute(final Vertex vertex, final Messenger messenger,
                           final GremlinTracker tracker,
                           final Gremlin gremlin) {
        if (this.holder.isDone()) {
            MapHelper.incr(tracker.getDoneGraphHolders(), this.holder.get(), this.holder);
            return false;
        }

        final Pipe<?, ?> pipe = PipelineHelper.getAs(this.holder.getFuture(), gremlin);
        if (this.destination.equals(Destination.VERTEX))
            this.holder.set(vertex);
        else if (this.destination.equals(Destination.EDGE)) {
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

        MapHelper.incr(tracker.getGraphHolders(), this.holder.get(), this.holder);
        pipe.addStarts(new SingleIterator(this.holder));
        return GremlinVertexProgram.processPipe(pipe, vertex, messenger, tracker);
    }

    // TODO: WHY IS THIS NOT LIKE FAUNUS WITH A BOTH?
    // TODO: I KNOW WHY -- CAUSE OF HOSTING VERTICES IS BOTH IN/OUT WHICH IS NECESSARY FOR EDGE MUTATIONS
    private Optional<Edge> getEdge(final Vertex vertex) {
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
}