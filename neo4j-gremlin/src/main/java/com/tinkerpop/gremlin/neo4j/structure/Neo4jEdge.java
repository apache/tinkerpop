package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.graphdb.NotFoundException;
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
        this.graph.tx().readWrite();
        if (direction.equals(Direction.OUT))
            return new Neo4jVertex(((Relationship) this.rawElement).getStartNode(), this.graph);
        else if (direction.equals(Direction.IN))
            return new Neo4jVertex(((Relationship) this.rawElement).getEndNode(), this.graph);
        else
            throw Edge.Exceptions.bothIsNotSupported();
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        try {
            ((Relationship) rawElement).delete();
        } catch (NotFoundException nfe) {
            // this one happens if the edge is committed
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.getId());
        } catch (IllegalStateException ise) {
            // this one happens if the edge is still chilling in the tx
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.getId());
        }
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
