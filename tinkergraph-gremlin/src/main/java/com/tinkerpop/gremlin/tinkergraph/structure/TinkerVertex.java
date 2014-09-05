package com.tinkerpop.gremlin.tinkergraph.structure;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.tinkergraph.process.graph.TinkerElementTraversal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerVertex extends TinkerElement implements Vertex {

    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    protected TinkerVertex(final Object id, final String label, final TinkerGraph graph) {
        super(id, label, graph);
    }

    @Override
    public <V> MetaProperty<V> property(final String key) {
        if (TinkerHelper.inComputerMode(this.graph)) {
            final List list = this.graph.graphView.getProperty(this, key);
            if (list.size() == 0)
                return MetaProperty.<V>empty();
            else if (list.size() == 1)
                return (MetaProperty<V>) list.get(0);
            else
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
        } else {
            if (this.properties.containsKey(key)) {
                final List<MetaProperty> list = (List) this.properties.get(key);
                if (list.size() > 1)
                    throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                else
                    return list.get(0);
            } else
                return MetaProperty.<V>empty();
        }
    }

    @Override
    public <V> MetaProperty<V> property(final String key, final V value) {
        if (this.graph.graphView != null && this.graph.graphView.getInUse()) {
            return (MetaProperty<V>) this.graph.graphView.setProperty(this, key, value);
        } else {
            ElementHelper.validateProperty(key, value);
            final MetaProperty<V> newProperty = new TinkerMetaProperty<>(this, key, value);
            final List<MetaProperty> list = (List<MetaProperty>) this.properties.getOrDefault(key, new ArrayList<MetaProperty>());
            list.add(newProperty);
            this.properties.put(key, list);
            this.graph.vertexIndex.autoUpdate(key, value, null, this);
            return newProperty;
        }
    }


    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        return TinkerHelper.addEdge(this.graph, this, (TinkerVertex) vertex, label, keyValues);
    }

    @Override
    public void remove() {
        this.bothE().forEach(Edge::remove);
        this.properties.clear();
        this.graph.vertexIndex.removeElement(this);
        this.graph.vertices.remove(this.id);
    }

    @Override
    public Vertex.Iterators iterators() {
        return this.iterators;
    }

    private final Vertex.Iterators iterators = new Iterators(this);

    protected class Iterators extends TinkerElement.Iterators implements Vertex.Iterators {

        public Iterators(final TinkerVertex vertex) {
            super(vertex);
        }

        @Override
        public <V> Iterator<MetaProperty<V>> properties(final String... propertyKeys) {
            return (Iterator) super.properties(propertyKeys);
        }

        @Override
        public <V> Iterator<MetaProperty<V>> hiddens(final String... propertyKeys) {
            return (Iterator) super.hiddens(propertyKeys);
        }

        @Override
        public Iterator<Edge> edges(final Direction direction, final int branchFactor, final String... labels) {
            return (Iterator) TinkerHelper.getEdges((TinkerVertex) this.element, direction, branchFactor, labels);
        }

        @Override
        public Iterator<Vertex> vertices(final Direction direction, final int branchFactor, final String... labels) {
            return (Iterator) TinkerHelper.getVertices((TinkerVertex) this.element, direction, branchFactor, labels);
        }
    }

    public GraphTraversal<Vertex, Vertex> start() {
        return new TinkerElementTraversal<>(this, this.graph);
    }


}
