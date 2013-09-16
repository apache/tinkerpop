package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexQuery extends DefaultVertexQuery {

    public TinkerVertex vertex;

    public TinkerVertexQuery(final TinkerVertex vertex) {
        this.vertex = vertex;
    }

    private Stream<Edge> makeStream() {
        Stream<Edge> edges = Stream.empty();
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.IN)) {
            edges = Stream.of(this.labels).flatMap(s -> this.vertex.inEdges.get(s).stream()).filter(v -> HasContainer.testAll(v, this.hasContainers));
        }
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.OUT)) {
            edges = Stream.concat(edges, Stream.of(this.labels).flatMap(s -> this.vertex.outEdges.get(s).stream()).filter(v -> HasContainer.testAll(v, this.hasContainers)));
        }
        return edges;
    }

    public Iterable<Edge> edges() {
        return this.makeStream().collect(Collectors.<Edge>toList());
    }

    public Iterable<Vertex> vertices() {
        Stream<Vertex> vertices = Stream.empty();
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.IN)) {
            vertices = Stream.of(this.labels).flatMap(s -> this.vertex.inEdges.get(s).stream()).filter(v -> HasContainer.testAll(v, this.hasContainers)).map(e -> e.getVertex(Direction.OUT));
        }
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.OUT)) {
            vertices = Stream.concat(vertices, Stream.of(this.labels).flatMap(s -> this.vertex.outEdges.get(s).stream()).filter(v -> HasContainer.testAll(v, this.hasContainers)).map(e -> e.getVertex(Direction.IN)));
        }
        return vertices.collect(Collectors.<Vertex>toList());
    }

    public long count() {
        return this.makeStream().count();
    }


}
