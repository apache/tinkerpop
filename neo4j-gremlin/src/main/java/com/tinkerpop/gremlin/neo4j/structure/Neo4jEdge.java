package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.graphdb.Relationship;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jEdge extends Neo4jElement implements Edge {

    public Neo4jEdge(final Relationship relationship, final Neo4jGraph graph) {
        super(graph);
        this.rawElement = relationship;
    }

    @Override
    public Vertex getVertex(final Direction direction) throws IllegalArgumentException {
        this.graph.autoStartTransaction(false);
        if (direction.equals(Direction.OUT))
            return new Neo4jVertex(((Relationship) this.rawElement).getStartNode(), this.graph);
        else if (direction.equals(Direction.IN))
            return new Neo4jVertex(((Relationship) this.rawElement).getEndNode(), this.graph);
        else
            throw Edge.Exceptions.bothIsNotSupported();
    }

    @Override
    public void remove() {
        // todo: how to throw this: throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.getId());
        this.graph.autoStartTransaction(true);
        ((Relationship) rawElement).delete();
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
