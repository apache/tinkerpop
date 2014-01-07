package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.query.VertexQuery;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.StringFactory;
import org.javatuples.Triplet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TinkerVertex extends TinkerElement implements Vertex {

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    protected TinkerVertex(final String id, final String label, final TinkerGraph graph) {
        super(id, label, graph);
        this.state = TinkerGraphComputer.State.STANDARD;
        this.centricId = id;
    }

    private TinkerVertex(final TinkerVertex vertex, final TinkerGraphComputer.State state, final String centricId, final TinkerVertexMemory annotationMemory) {
        super(vertex.id, vertex.label, vertex.graph);
        this.state = state;
        this.outEdges = vertex.outEdges;
        this.inEdges = vertex.inEdges;
        this.properties = vertex.properties;
        this.vertexMemory = annotationMemory;
        this.centricId = centricId;
    }

    public <V> void setProperty(final String key, final V value) {
        if (TinkerGraphComputer.State.STANDARD == this.state) {
            ElementHelper.validateProperty(key, value);
            final Property oldProperty = super.getProperty(key);
            this.properties.put(key, new TinkerProperty<>(this, key, value));
            this.graph.vertexIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.getValue() : null, this);
        } else if (TinkerGraphComputer.State.CENTRIC == this.state) {
            ElementHelper.validateProperty(key, value);
            if (this.vertexMemory.getComputeKeys().containsKey(key))
                this.vertexMemory.setProperty(this, key, value);
            else
                throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        } else {
            throw GraphComputer.Exceptions.adjacentElementPropertiesCanNotBeWritten();
        }
    }

    public VertexQuery query() {
        return new TinkerVertexQuery(this, this.vertexMemory);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        final Triplet<String, Vertex, Object[]> targs = Triplet.with(label, vertex, keyValues);
        final Triplet<String, Vertex, Object[]> strategizedTargs = this.graph.strategy()
                .ifPresent(s->s.getPreAddEdge().apply(targs), targs);
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) strategizedTargs.getValue1(),
                strategizedTargs.getValue0(), strategizedTargs.getValue2());
    }

    public void remove() {
        if (!graph.vertices.containsKey(this.id))
            throw Element.Exceptions.elementHasAlreadyBeenRemovedOrDoesNotExist(Vertex.class, this.getId());

        this.query().direction(Direction.BOTH).edges().forEach(Edge::remove);
        this.properties.clear();
        graph.vertexIndex.removeElement(this);
        graph.vertices.remove(this.id);
    }

    public TinkerVertex createClone(final TinkerGraphComputer.State state, final String centricId, final TinkerVertexMemory vertexMemory) {
        return new TinkerVertex(this, state, centricId, vertexMemory);
    }
}
