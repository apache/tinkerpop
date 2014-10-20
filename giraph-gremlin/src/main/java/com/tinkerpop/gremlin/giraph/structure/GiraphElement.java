package com.tinkerpop.gremlin.giraph.structure;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GiraphElement implements Element {

    protected TinkerElement tinkerElement;
    protected GiraphGraph graph;

    protected GiraphElement() {
    }

    protected GiraphElement(final TinkerElement tinkerElement, final GiraphGraph graph) {
        this.tinkerElement = tinkerElement;
        this.graph = graph;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Object id() {
        return this.tinkerElement.id();
    }

    @Override
    public String label() {
        return this.tinkerElement.label();
    }

    @Override
    public void remove() {
        if (this.tinkerElement instanceof Vertex)
            throw Vertex.Exceptions.vertexRemovalNotSupported();
        else
            throw Edge.Exceptions.edgeRemovalNotSupported();
    }


    @Override
    public <V> Property<V> property(final String key) {
        return this.tinkerElement.property(key);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return this.tinkerElement.hashCode();
    }

    @Override
    public String toString() {
        return this.tinkerElement.toString();
    }
}
