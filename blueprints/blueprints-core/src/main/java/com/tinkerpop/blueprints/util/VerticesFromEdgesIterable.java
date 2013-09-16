package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.Iterator;

/**
 * VerticesFromEdgesIterable is a helper class that returns vertices that meet the direction/label criteria of the incident edges.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VerticesFromEdgesIterable implements Iterable<Vertex> {

    private final Iterable<Edge> iterable;
    private final Direction direction;
    private final Vertex vertex;

    public VerticesFromEdgesIterable(final Vertex vertex, final Direction direction, final String... labels) {
        this.direction = direction;
        this.vertex = vertex;
        this.iterable = vertex.query().direction(direction).labels(labels).edges();
    }

    public Iterator<Vertex> iterator() {
        return new Iterator<Vertex>() {
            final Iterator<Edge> itty = iterable.iterator();

            public void remove() {
                this.itty.remove();
            }

            public boolean hasNext() {
                return this.itty.hasNext();
            }

            public Vertex next() {
                if (direction.equals(Direction.OUT)) {
                    return this.itty.next().getVertex(Direction.IN);
                } else if (direction.equals(Direction.IN)) {
                    return this.itty.next().getVertex(Direction.OUT);
                } else {
                    final Edge edge = this.itty.next();
                    if (edge.getVertex(Direction.IN).equals(vertex))
                        return edge.getVertex(Direction.OUT);
                    else
                        return edge.getVertex(Direction.IN);
                }
            }
        };
    }
}
