package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jEdge extends Neo4jElement implements Edge, WrappedEdge<Relationship> {

    public Neo4jEdge(final Relationship relationship, final Neo4jGraph graph) {
        super(graph);
        this.baseElement = relationship;
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        try {
            ((Relationship) baseElement).delete();
        } catch (NotFoundException ignored) {
            // this one happens if the edge is committed
        } catch (IllegalStateException ignored) {
            // this one happens if the edge is still chilling in the tx
        }
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        this.graph.tx().readWrite();
        return (Iterator) Neo4jHelper.getVertices(this, direction);
    }

    public GraphTraversal<Edge, Edge> start() {
        final GraphTraversal<Edge, Edge> traversal = new DefaultGraphTraversal<>(this.graph);
        return (GraphTraversal) traversal.addStep(new StartStep<>(traversal, this));
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    public Relationship getBaseEdge() {
        return (Relationship) this.baseElement;
    }
}
