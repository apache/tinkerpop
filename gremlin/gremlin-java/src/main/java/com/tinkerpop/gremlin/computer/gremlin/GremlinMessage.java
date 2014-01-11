package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.pipes.util.Holder;

import java.io.Serializable;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GremlinMessage implements Serializable {

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

    protected final Object elementId;
    protected final Destination destination;
    protected final String propertyKey;
    protected Holder holder;

    protected GremlinMessage(final Destination destination, final Object elementId, final String propertyKey, final Holder holder) {
        this.destination = destination;
        this.elementId = elementId;
        this.propertyKey = propertyKey;
        this.holder = holder;
        this.holder.getPath().microSize();
    }

    ///////////////////////////////////

    protected boolean stageHolder(final Vertex vertex) {
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

    protected Optional<Edge> getEdge(final Vertex vertex) {
        // TODO: WHY IS THIS NOT LIKE FAUNUS WITH A BOTH?
        // TODO: I KNOW WHY -- CAUSE OF HOSTING VERTICES IS BOTH IN/OUT WHICH IS NECESSARY FOR EDGE MUTATIONS
        return StreamFactory.stream(vertex.query().direction(Direction.OUT).edges())
                .filter(e -> e.getId().equals(this.elementId))
                .findFirst();
    }

    protected Optional<Property> getProperty(final Vertex vertex) {
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