package com.tinkerpop.tinkergraph;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.process.olap.GraphComputer;
import com.tinkerpop.gremlin.structure.query.util.DefaultVertexQuery;
import com.tinkerpop.gremlin.structure.query.util.HasContainer;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

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
    protected final TinkerVertexMemory vertexMemory;

    public TinkerVertexQuery(final TinkerVertex vertex, final TinkerVertexMemory vertexMemory) {
        this.vertex = vertex;
        this.vertexMemory = vertexMemory;
    }

    private Stream<TinkerEdge> makeStream() {
        Stream<TinkerEdge> edges = Stream.empty();

        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.IN)) {
            edges = (Stream) this.getInEdges(this.labels).stream();
            if (!this.adjacents.isEmpty())
                edges = edges.filter(e -> this.adjacents.contains(e.getVertex(Direction.OUT)));
        }
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.OUT)) {
            edges = (Stream) Stream.concat(edges, this.getOutEdges(this.labels).stream());
            if (!this.adjacents.isEmpty())
                edges = edges.filter(e -> this.adjacents.contains(e.getVertex(Direction.IN)));
        }
        edges = edges.filter(e -> HasContainer.testAll(e, this.hasContainers));
        if (this.limit != Integer.MAX_VALUE)
            edges = edges.limit(this.limit);

        // GENERATE COMPUTE SHELLED EDGES DURING GRAPH COMPUTING
        if (TinkerGraphComputer.State.CENTRIC == this.vertex.state)
            edges = edges.map(e -> e.createClone(TinkerGraphComputer.State.CENTRIC, this.vertex.getId(), this.vertexMemory));
        return edges;
    }

    public Iterable<Edge> edges() {
        return (Iterable) StreamFactory.iterable(this.makeStream());
    }

    public Iterable<Vertex> vertices() {
        if (this.vertex.state.equals(TinkerGraphComputer.State.ADJACENT))
            throw GraphComputer.Exceptions.adjacentVerticesCanNotBeQueried();

        Stream<TinkerVertex> vertices = Stream.empty();
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.IN))
            vertices = (Stream) Stream.concat(vertices, this.getInEdges(this.labels).stream().filter(e -> HasContainer.testAll(e, this.hasContainers)).map(e -> e.getVertex(Direction.OUT)));
        if (this.direction.equals(Direction.BOTH) || this.direction.equals(Direction.OUT))
            vertices = (Stream) Stream.concat(vertices, this.getOutEdges(this.labels).stream().filter(e -> HasContainer.testAll(e, this.hasContainers)).map(e -> e.getVertex(Direction.IN)));
        if (!this.adjacents.isEmpty())
            vertices = vertices.filter(this.adjacents::contains);
        vertices = vertices.limit(this.limit);

        // GENERATE COMPUTE SHELLED ADJACENT VERTICES DURING GRAPH COMPUTING
        if (TinkerGraphComputer.State.CENTRIC == this.vertex.state)
            vertices = vertices.map(v -> v.createClone(TinkerGraphComputer.State.ADJACENT, this.vertex.getId(), this.vertexMemory));

        return (Iterable) StreamFactory.iterable(vertices);
    }

    public long count() {
        return this.makeStream().count();
    }

    private List<Edge> getInEdges(final String... labels) {
        if (labels.length == 0) {
            final List<Edge> totalEdges = new ArrayList<>();
            for (final Collection<Edge> edges : this.vertex.inEdges.values())
                totalEdges.addAll(edges);
            return totalEdges;
        } else if (labels.length == 1) {
            final Set<Edge> edges = this.vertex.inEdges.get(labels[0]);
            return null == edges ? Collections.emptyList() : new ArrayList<>(edges);
        } else {
            final List<Edge> totalEdges = new ArrayList<>();
            for (final String label : labels) {
                final Set<Edge> edges = this.vertex.inEdges.get(label);
                if (null != edges)
                    totalEdges.addAll(edges);
            }
            return totalEdges;
        }
    }

    private List<Edge> getOutEdges(final String... labels) {
        if (labels.length == 0) {
            final List<Edge> totalEdges = new ArrayList<>();
            for (final Collection<Edge> edges : this.vertex.outEdges.values())
                totalEdges.addAll(edges);
            return totalEdges;
        } else if (labels.length == 1) {
            final Set<Edge> edges = this.vertex.outEdges.get(labels[0]);
            return null == edges ? Collections.emptyList() : new ArrayList<>(edges);
        } else {
            final List<Edge> totalEdges = new ArrayList<>();
            for (final String label : labels) {
                final Set<Edge> edges = this.vertex.outEdges.get(label);
                if (null != edges)
                    totalEdges.addAll(edges);
            }
            return totalEdges;
        }
    }
}
