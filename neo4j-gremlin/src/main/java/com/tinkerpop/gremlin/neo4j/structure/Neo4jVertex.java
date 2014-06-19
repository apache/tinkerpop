package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.step.map.Neo4jVertexStep;
import com.tinkerpop.gremlin.process.graph.DefaultGraphTraversal;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.StartStep;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
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
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw Edge.Exceptions.userSuppliedIdsNotSupported();

        this.graph.tx().readWrite();
        final Node node = (Node) this.rawElement;
        final Neo4jEdge edge = new Neo4jEdge(node.createRelationshipTo(((Neo4jVertex) inVertex).getRawVertex(),
                DynamicRelationshipType.withName(label)), this.graph);
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    @Override
    public GraphTraversal<Vertex, Vertex> to(final Direction direction, final int branchFactor, final String... labels) {
        this.graph.tx().readWrite();
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new Neo4jVertexStep(this.graph, traversal, Vertex.class, direction, branchFactor, labels));
        return traversal;
    }

    @Override
    public GraphTraversal<Vertex, Edge> toE(final Direction direction, final int branchFactor, final String... labels) {
        this.graph.tx().readWrite();
        final GraphTraversal<Vertex, Edge> traversal = new DefaultGraphTraversal<>();
        traversal.addStep(new StartStep<Vertex>(traversal, this));
        traversal.addStep(new Neo4jVertexStep(this.graph, traversal, Edge.class, direction, branchFactor, labels));
        return traversal;
    }

    public Node getRawVertex() {
        return (Node) this.rawElement;
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }
}
