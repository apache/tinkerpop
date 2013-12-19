package com.tinkerpop.gremlin.computer.gremlin;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GremlinMessage implements Serializable {

    public enum Destination {VERTEX, EDGE, PROPERTY}

    public final Object elementId;
    public final Destination destination;

    public GremlinMessage(final Destination destination, final Object elementId) {
        this.elementId = elementId;
        this.destination = destination;
    }

    public static GremlinMessage of(final Destination destination, final Object elementId) {
        return new GremlinMessage(destination, elementId);
    }

    public static GremlinMessage of(final Element element) {
        Destination d;
        if (element instanceof Vertex)
            d = Destination.VERTEX;
        else if (element instanceof Edge)
            d = Destination.EDGE;
        else
            d = Destination.PROPERTY;
        return new GremlinMessage(d, element.getId());
    }

    public static Vertex getVertex(final Element element) {
        if (element instanceof Vertex)
            return (Vertex) element;
        else if (element instanceof Edge)
            return ((Edge) element).getVertex(Direction.IN);
        else
            throw new UnsupportedOperationException("Properties not supported yet");
    }

}