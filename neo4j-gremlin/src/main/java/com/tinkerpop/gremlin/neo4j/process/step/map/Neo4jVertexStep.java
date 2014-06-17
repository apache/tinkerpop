package com.tinkerpop.gremlin.neo4j.process.step.map;

import com.tinkerpop.gremlin.neo4j.structure.Neo4jEdge;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jGraph;
import com.tinkerpop.gremlin.neo4j.structure.Neo4jVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jVertexStep<E extends Element> extends VertexStep<E> {

    public Neo4jVertexStep(final Neo4jGraph g, final Traversal traversal, final Class<E> returnClass,
                           final com.tinkerpop.gremlin.structure.Direction direction,
                           final int branchFactor, final String... labels) {
        super(traversal, returnClass, direction, branchFactor, labels);
        if (Vertex.class.isAssignableFrom(returnClass)) {
            if (direction.equals(com.tinkerpop.gremlin.structure.Direction.OUT))
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new Neo4jVertexVertexIterable<>(g, ((Neo4jVertex) traverser.get()).getRawVertex(), Direction.OUTGOING, labels)).limit(this.branchFactor).iterator());
            else if (direction.equals(com.tinkerpop.gremlin.structure.Direction.IN))
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new Neo4jVertexVertexIterable<>(g, ((Neo4jVertex) traverser.get()).getRawVertex(), Direction.INCOMING, labels)).limit(this.branchFactor).iterator());
            else
                this.setFunction(traverser -> (Iterator) Stream.concat(StreamFactory.stream(new Neo4jVertexVertexIterable<>(g, ((Neo4jVertex) traverser.get()).getRawVertex(), Direction.OUTGOING, labels)),
                                                                    StreamFactory.stream(new Neo4jVertexVertexIterable<>(g, ((Neo4jVertex) traverser.get()).getRawVertex(), Direction.INCOMING, labels))).limit(this.branchFactor).iterator());
        } else {
            if (direction.equals(com.tinkerpop.gremlin.structure.Direction.OUT))
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new Neo4jVertexEdgeIterable<>(g, ((Neo4jVertex) traverser.get()).getRawVertex(), Direction.OUTGOING, labels)).limit(this.branchFactor).iterator());
            else if (direction.equals(com.tinkerpop.gremlin.structure.Direction.IN))
                this.setFunction(traverser -> (Iterator) StreamFactory.stream(new Neo4jVertexEdgeIterable<>(g, ((Neo4jVertex) traverser.get()).getRawVertex(), Direction.INCOMING, labels)).limit(this.branchFactor).iterator());
            else
                this.setFunction(traverser -> (Iterator) Stream.concat(StreamFactory.stream(new Neo4jVertexEdgeIterable<>(g, ((Neo4jVertex) traverser.get()).getRawVertex(), Direction.OUTGOING, labels)),
                                                                    StreamFactory.stream(new Neo4jVertexEdgeIterable<>(g, ((Neo4jVertex) traverser.get()).getRawVertex(), Direction.INCOMING, labels))).limit(this.branchFactor).iterator());
        }
    }


    private class Neo4jVertexVertexIterable<T extends Vertex> implements Iterable<Neo4jVertex> {
        private final Neo4jGraph graph;
        private final Node node;
        private final Direction direction;
        private final DynamicRelationshipType[] labels;

        public Neo4jVertexVertexIterable(final Neo4jGraph graph, final Node node, final Direction direction, final String... labels) {
            this.graph = graph;
            this.node = node;
            this.direction = direction;
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

    private class Neo4jVertexEdgeIterable<T extends Edge> implements Iterable<Neo4jEdge> {

        private final Neo4jGraph graph;
        private final Node node;
        private final Direction direction;
        private final DynamicRelationshipType[] labels;

        public Neo4jVertexEdgeIterable(final Neo4jGraph graph, final Node node, final Direction direction, final String... labels) {
            this.graph = graph;
            this.node = node;
            this.direction = direction;
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