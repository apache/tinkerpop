package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.util.DefaultVertexQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
            edges = this.getInEdges(this.labels).stream();
        }
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.OUT)) {
            edges = Stream.concat(edges, this.getOutEdges(this.labels).stream());
        }
        return edges.filter(e -> HasContainer.testAll(e, this.hasContainers)).limit(this.limit);
    }

    public Iterable<Edge> edges() {
        return this.makeStream().collect(Collectors.<Edge>toList());
    }

    public Iterable<Vertex> vertices() {
        Stream<Vertex> vertices = Stream.empty();

        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.IN)) {
            vertices = Stream.concat(vertices, this.getInEdges(this.labels).stream().filter(e -> HasContainer.testAll(e, this.hasContainers)).map(e -> e.getVertex(Direction.OUT)));
        }
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.OUT)) {
            vertices = Stream.concat(vertices, this.getOutEdges(this.labels).stream().filter(e -> HasContainer.testAll(e, this.hasContainers)).map(e -> e.getVertex(Direction.IN)));
        }
        return vertices.limit(this.limit).collect(Collectors.<Vertex>toList());
    }

    public long count() {
        return this.makeStream().count();
    }

    private List<Edge> getInEdges(final String... labels) {
        if (labels.length == 0) {
            final List<Edge> totalEdges = new ArrayList<>();
            for (final Collection<Edge> edges : this.vertex.inEdges.values()) {
                totalEdges.addAll(edges);
            }
            return totalEdges;
        } else if (labels.length == 1) {
            final Set<Edge> edges = this.vertex.inEdges.get(labels[0]);
            if (null == edges) {
                return Collections.emptyList();
            } else {
                return new ArrayList<>(edges);
            }
        } else {
            final List<Edge> totalEdges = new ArrayList<>();
            for (final String label : labels) {
                final Set<Edge> edges = this.vertex.inEdges.get(label);
                if (null != edges) {
                    totalEdges.addAll(edges);
                }
            }
            return totalEdges;
        }
    }

    private List<Edge> getOutEdges(final String... labels) {
        if (labels.length == 0) {
            final List<Edge> totalEdges = new ArrayList<>();
            for (final Collection<Edge> edges : this.vertex.outEdges.values()) {
                totalEdges.addAll(edges);
            }
            return totalEdges;
        } else if (labels.length == 1) {
            final Set<Edge> edges = this.vertex.outEdges.get(labels[0]);
            if (null == edges) {
                return Collections.emptyList();
            } else {
                return new ArrayList<>(edges);
            }
        } else {
            final List<Edge> totalEdges = new ArrayList<>();
            for (final String label : labels) {
                final Set<Edge> edges = this.vertex.outEdges.get(label);
                if (null != edges) {
                    totalEdges.addAll(edges);
                }
            }
            return totalEdges;
        }
    }


}
