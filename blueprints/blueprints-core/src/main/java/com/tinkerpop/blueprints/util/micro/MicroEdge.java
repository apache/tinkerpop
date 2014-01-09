package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MicroEdge extends MicroElement implements Edge {

    final MicroVertex outVertex;
    final MicroVertex inVertex;

    public MicroEdge(final Edge edge) {
        super(edge);
        this.outVertex = new MicroVertex(edge.getVertex(Direction.OUT));
        this.inVertex = new MicroVertex(edge.getVertex(Direction.IN));
    }

    public Vertex getVertex(Direction direction) {
        if (direction.equals(Direction.OUT))
            return outVertex;
        else if (direction.equals(Direction.IN))
            return inVertex;
        else
            throw Edge.Exceptions.bothIsNotSupported();
    }

    public String toString() {
        return StringFactory.edgeString(this) + ".";
    }
}
