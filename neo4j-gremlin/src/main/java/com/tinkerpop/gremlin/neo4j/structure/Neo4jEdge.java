package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.neo4j.process.graph.Neo4jEdgeTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
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
public class Neo4jEdge extends Neo4jElement implements Edge, Edge.Iterators, WrappedEdge<Relationship>, Neo4jEdgeTraversal {

    public Neo4jEdge(final Relationship relationship, final Neo4jGraph graph) {
        super(graph);
        this.baseElement = relationship;
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.getBaseEdge().getId());
        this.removed = true;
        this.graph.tx().readWrite();
        try {
            ((Relationship) baseElement).delete();
        } catch (IllegalStateException | NotFoundException ignored) {
            // NotFoundException happens if the edge is committed
            // IllegalStateException happens if the edge is still chilling in the tx
        }
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.getBaseEdge().getType().name();
    }

    @Override
    public Relationship getBaseEdge() {
        return (Relationship) this.baseElement;
    }

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }

    @Override
    public <V> Iterator<Property<V>> hiddenPropertyIterator(final String... propertyKeys) {
        return (Iterator) super.hiddenPropertyIterator(propertyKeys);
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        graph.tx().readWrite();
        return (Iterator) Neo4jHelper.getVertices(Neo4jEdge.this, direction);
    }
}