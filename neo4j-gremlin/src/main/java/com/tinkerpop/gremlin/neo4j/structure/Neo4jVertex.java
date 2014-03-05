package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jVertex extends Neo4jElement implements Vertex {
    public Neo4jVertex(final Node node, final Neo4jGraph graph) {
        super(graph);
        this.rawElement = node;
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();

        try {
            final Node node = (Node) this.rawElement;
            for (final Relationship relationship : node.getRelationships(org.neo4j.graphdb.Direction.BOTH)) {
                relationship.delete();
            }
            node.delete();
        } catch (NotFoundException nfe) {
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, ((Node) this.rawElement).getId());
        } catch (IllegalStateException ise) {
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, ((Node) this.rawElement).getId());
        }
    }

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (label == null)
            throw Edge.Exceptions.edgeLabelCanNotBeNull();

        this.graph.tx().readWrite();
        final Node node = (Node) this.rawElement;
        final Neo4jEdge edge = new Neo4jEdge(node.createRelationshipTo(((Neo4jVertex) inVertex).getRawVertex(),
                DynamicRelationshipType.withName(label)), this.graph);
        edge.setProperties(keyValues);
        return edge;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> out(int branchFactor, String... labels) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GraphTraversal<Vertex, Vertex> in(int branchFactor, String... labels) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GraphTraversal<Vertex, Vertex> both(int branchFactor, String... labels) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GraphTraversal<Vertex, Edge> outE(int branchFactor, String... labels) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GraphTraversal<Vertex, Edge> inE(int branchFactor, String... labels) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public GraphTraversal<Vertex, Edge> bothE(int branchFactor, String... labels) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Node getRawVertex() {
        return (Node) this.rawElement;
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }
}
