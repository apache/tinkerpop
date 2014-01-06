package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.computer.gremlin.util.MicroEdge;
import com.tinkerpop.gremlin.computer.gremlin.util.MicroElement;
import com.tinkerpop.gremlin.computer.gremlin.util.MicroProperty;
import com.tinkerpop.gremlin.computer.gremlin.util.MicroVertex;
import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.Path;

import java.io.Serializable;

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
        this.microSizePath();
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

    /*public static GremlinMessage of(final Object object, final long counts) {
        Destination destination = Destination.of(object);
        if (destination == Destination.VERTEX)
            return new GremlinMessage(destination, ((Vertex) object).getId(), null, counts);
        else if (destination == Destination.EDGE)
            return new GremlinMessage(destination, ((Edge) object).getId(), null, counts);
        else
            return new GremlinMessage(destination, ((Property) object).getElement().getId(), ((Property) object).getKey(), counts);
    }*/

    public Holder getHolder() {
        return this.holder;
    }

    private void microSizePath() {
        final Path newPath = new Path();
        this.holder.getPath().forEach((a, b) -> {
            if (b instanceof MicroElement || b instanceof MicroProperty) {
                newPath.add(a, b);
            } else if (b instanceof Vertex) {
                newPath.add(a, new MicroVertex((Vertex) b));
            } else if (b instanceof Edge) {
                newPath.add(a, new MicroEdge((Edge) b));
            } else if (b instanceof Property) {
                newPath.add(a, new MicroProperty((Property) b));
            } else {
                newPath.add(a, b);
            }
        });
        this.holder.setPath(newPath);
    }
}