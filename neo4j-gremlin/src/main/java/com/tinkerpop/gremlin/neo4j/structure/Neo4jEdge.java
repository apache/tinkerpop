package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.map.Neo4jEdgeVertexStep;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
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
    public void remove() {
        this.graph.tx().readWrite();
        try {
            ((Relationship) rawElement).delete();
        } catch (NotFoundException nfe) {
            // this one happens if the edge is committed
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.id());
        } catch (IllegalStateException ise) {
            // this one happens if the edge is still chilling in the tx
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Edge.class, this.id());
        }
    }

    @Override
    public GraphTraversal<Edge, Vertex> inV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new Neo4jEdgeVertexStep(traversal, this.graph, Direction.IN));
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Vertex> outV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new Neo4jEdgeVertexStep(traversal, this.graph, Direction.OUT));
        return traversal;
    }

    @Override
    public GraphTraversal<Edge, Vertex> bothV() {
        final GraphTraversal traversal = this.start();
        traversal.addStep(new Neo4jEdgeVertexStep(traversal, this.graph, Direction.BOTH));
        return traversal;
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
