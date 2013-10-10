package com.tinkerpop.blueprints.computer.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.VertexSystemMemory;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.query.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AdjacentShellVertexQuery extends DefaultVertexQuery {

    private final Vertex baseVertex;
    private final Vertex coreVertex;
    private final VertexSystemMemory vertexMemory;

    public AdjacentShellVertexQuery(AdjacentShellVertex adjacentVertex, VertexSystemMemory vertexMemory) {
        this.baseVertex = adjacentVertex.baseVertex;
        this.coreVertex = adjacentVertex.coreVertex;
        this.vertexMemory = vertexMemory;
    }

    public Iterable<Vertex> vertices() {
        final VertexQuery query = this.baseVertex.query().direction(this.direction).labels(this.labels);
        for (HasContainer hasContainer : this.hasContainers) {
            query.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
        }
        return new Iterable<Vertex>() {
            @Override
            public Iterator<Vertex> iterator() {
                final Iterator<Vertex> stream = StreamFactory.stream(query.vertices()).filter(v -> v.getId().equals(coreVertex.getId())).iterator();
                return new Iterator<Vertex>() {
                    @Override
                    public boolean hasNext() {
                        return stream.hasNext();
                    }

                    @Override
                    public Vertex next() {
                        return new CoreShellVertex(stream.next(), vertexMemory);
                    }
                };
            }
        };

    }

    public Iterable<Edge> edges() {
        final VertexQuery query = baseVertex.query().direction(this.direction).labels(this.labels);
        for (HasContainer hasContainer : this.hasContainers) {
            query.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
        }
        return new Iterable<Edge>() {
            @Override
            public Iterator<Edge> iterator() {
                final Iterator<Edge> stream = StreamFactory.stream(query.edges()).filter(e -> e.getVertex(direction.opposite()).equals(coreVertex)).iterator();
                return new Iterator<Edge>() {
                    @Override
                    public boolean hasNext() {
                        return stream.hasNext();
                    }

                    @Override
                    public Edge next() {
                        return new ShellEdge(stream.next(), vertexMemory);
                    }
                };
            }
        };
    }

    public long count() {
        return StreamFactory.stream(this.edges()).count();
    }
}
