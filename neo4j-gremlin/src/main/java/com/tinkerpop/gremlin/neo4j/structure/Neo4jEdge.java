package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jEdgeTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jEdge extends Neo4jElement implements Edge, WrappedEdge<Relationship>, Neo4jEdgeTraversal {

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

    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Relationship getBaseEdge() {
        return (Relationship) this.baseElement;
    }

    @Override
    public Edge.Iterators iterators() {
        return this.iterators;
    }

    private final Edge.Iterators iterators = new Iterators(this);

    protected class Iterators extends Neo4jElement.Iterators implements Edge.Iterators {

        public Iterators(final Neo4jEdge edge) {
            super(edge);
        }

        @Override
        public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
            return (Iterator) super.properties(propertyKeys);
        }

        @Override
        public <V> Iterator<Property<V>> hiddens(final String... propertyKeys) {
            return (Iterator) super.hiddens(propertyKeys);
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction) {
            graph.tx().readWrite();
            return (Iterator) Neo4jHelper.getVertices((Neo4jEdge) this.element, direction);
        }

    }
}