package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jHelper {

    public static org.neo4j.graphdb.Direction mapDirection(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return org.neo4j.graphdb.Direction.OUTGOING;
        else if (direction.equals(Direction.IN))
            return org.neo4j.graphdb.Direction.INCOMING;
        else
            return org.neo4j.graphdb.Direction.BOTH;
    }

    public static Iterator<Neo4jVertex> getVertices(final Neo4jVertex vertex, final Direction direction, final String... labels) {
        return new Neo4jVertexVertexIterable<>(vertex, direction, labels).iterator();
    }

    public static Iterator<Neo4jEdge> getEdges(final Neo4jVertex vertex, final Direction direction, final String... labels) {
        return new Neo4jVertexEdgeIterable<>(vertex, direction, labels).iterator();
    }

    public static Iterator<Neo4jVertex> getVertices(final Neo4jEdge edge, final Direction direction) {
        final List<Neo4jVertex> vertices = new ArrayList<>(2);
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH))
            vertices.add(new Neo4jVertex(((Relationship) edge.getRawElement()).getStartNode(), edge.graph));
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH))
            vertices.add(new Neo4jVertex(((Relationship) edge.getRawElement()).getEndNode(), edge.graph));
        return vertices.iterator();
    }

    private static class Neo4jVertexVertexIterable<T extends Vertex> implements Iterable<Neo4jVertex> {
        private final Neo4jGraph graph;
        private final Node node;
        private final org.neo4j.graphdb.Direction direction;
        private final DynamicRelationshipType[] labels;

        public Neo4jVertexVertexIterable(final Neo4jVertex vertex, final Direction direction, final String... labels) {
            this.graph = vertex.graph;
            this.node = vertex.getRawVertex();
            this.direction = mapDirection(direction);
            this.labels = new DynamicRelationshipType[labels.length];
            for (int i = 0; i < labels.length; i++) {
                this.labels[i] = DynamicRelationshipType.withName(labels[i]);
            }
        }

        public Iterator<Neo4jVertex> iterator() {
            final Iterator<Relationship> itty;
            if (labels.length > 0)
                itty = node.getRelationships(direction, labels).iterator();
            else
                itty = node.getRelationships(direction).iterator();

            return new Iterator<Neo4jVertex>() {
                public Neo4jVertex next() {
                    return new Neo4jVertex(itty.next().getOtherNode(node), graph);
                }

                public boolean hasNext() {
                    return itty.hasNext();
                }

                public void remove() {
                    itty.remove();
                }
            };
        }
    }

    private static class Neo4jVertexEdgeIterable<T extends Edge> implements Iterable<Neo4jEdge> {

        private final Neo4jGraph graph;
        private final Node node;
        private final org.neo4j.graphdb.Direction direction;
        private final DynamicRelationshipType[] labels;

        public Neo4jVertexEdgeIterable(final Neo4jVertex vertex, final Direction direction, final String... labels) {
            this.graph = vertex.graph;
            this.node = vertex.getRawVertex();
            this.direction = mapDirection(direction);
            this.labels = new DynamicRelationshipType[labels.length];
            for (int i = 0; i < labels.length; i++) {
                this.labels[i] = DynamicRelationshipType.withName(labels[i]);
            }
        }

        public Iterator<Neo4jEdge> iterator() {
            final Iterator<Relationship> itty;
            if (labels.length > 0)
                itty = node.getRelationships(direction, labels).iterator();
            else
                itty = node.getRelationships(direction).iterator();

            return new Iterator<Neo4jEdge>() {
                public Neo4jEdge next() {
                    return new Neo4jEdge(itty.next(), graph);
                }

                public boolean hasNext() {
                    return itty.hasNext();
                }

                public void remove() {
                    itty.remove();
                }
            };
        }
    }


}
