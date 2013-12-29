package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertexQuery extends DefaultVertexQuery {

    protected final TinkerVertex vertex;
    protected final TinkerAnnotationMemory annotationMemory;

    public TinkerVertexQuery(final TinkerVertex vertex, final TinkerAnnotationMemory annotationMemory) {
        this.vertex = vertex;
        this.annotationMemory = annotationMemory;
    }

    private Stream<TinkerEdge> makeStream() {
        Stream<TinkerEdge> edges = Stream.empty();

        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.IN)) {
            edges = (Stream) this.getInEdges(this.labels).stream();
        }
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.OUT)) {
            edges = (Stream) Stream.concat(edges, this.getOutEdges(this.labels).stream());
        }
        edges = edges.filter(e -> HasContainer.testAll(e, this.hasContainers)).limit(this.limit);

        // GENERATE COMPUTE SHELLED EDGES DURING GRAPH COMPUTING
        if (TinkerGraphComputer.State.CENTRIC == this.vertex.state)
            edges = edges.map(e -> e.createClone(TinkerGraphComputer.State.CENTRIC, this.vertex.getId(), this.annotationMemory));
        return edges;
    }

    public Iterable<Edge> edges() {
        return (Iterable) StreamFactory.iterable(this.makeStream());
    }

    public Iterable<Vertex> vertices() {
        if (this.vertex.state.equals(TinkerGraphComputer.State.ADJACENT))
            throw GraphComputer.Exceptions.adjacentVerticesCanNotBeQueried();

        Stream<TinkerVertex> vertices = Stream.empty();
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.IN)) {
            vertices = (Stream) Stream.concat(vertices, this.getInEdges(this.labels).stream().filter(e -> HasContainer.testAll(e, this.hasContainers)).map(e -> e.getVertex(Direction.OUT)));
        }
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.OUT)) {
            vertices = (Stream) Stream.concat(vertices, this.getOutEdges(this.labels).stream().filter(e -> HasContainer.testAll(e, this.hasContainers)).map(e -> e.getVertex(Direction.IN)));
        }
        vertices = vertices.limit(this.limit);

        // GENERATE COMPUTE SHELLED ADJACENT VERTICES DURING GRAPH COMPUTING
        if (TinkerGraphComputer.State.CENTRIC == this.vertex.state)
            vertices = vertices.map(v -> v.createClone(TinkerGraphComputer.State.ADJACENT, this.vertex.getId(), this.annotationMemory));

        return (Iterable) StreamFactory.iterable(vertices);
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
