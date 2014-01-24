package com.tinkerpop.blueprints.util.micro;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.StreamFactory;
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

    public Vertex getVertex(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return outVertex;
        else if (direction.equals(Direction.IN))
            return inVertex;
        else
            throw Edge.Exceptions.bothIsNotSupported();
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    public Edge inflate(final Vertex hostVertex) {
        return StreamFactory.stream(hostVertex.query().direction(Direction.OUT).labels(this.label).edges())
                .filter(e -> e.getId().equals(this.id))
                .findFirst().get();
    }

    public Edge inflate(final Graph graph) {
        return graph.query().ids(this.id).edges().iterator().next();
    }

    public static MicroEdge deflate(final Edge edge) {
        return new MicroEdge(edge);
    }
}
