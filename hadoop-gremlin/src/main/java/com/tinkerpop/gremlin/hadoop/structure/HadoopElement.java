package com.tinkerpop.gremlin.hadoop.structure;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class HadoopElement implements Element {

    protected Element baseElement;
    protected HadoopGraph graph;

    protected HadoopElement() {
    }

    protected HadoopElement(final Element baseElement, final HadoopGraph graph) {
        this.baseElement = baseElement;
        this.graph = graph;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Object id() {
        return this.baseElement.id();
    }

    @Override
    public String label() {
        return this.baseElement.label();
    }

    @Override
    public void remove() {
        if (this.baseElement instanceof Vertex)
            throw Vertex.Exceptions.vertexRemovalNotSupported();
        else
            throw Edge.Exceptions.edgeRemovalNotSupported();
    }


    @Override
    public <V> Property<V> property(final String key) {
        return this.baseElement.property(key);
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
        return this.baseElement.hashCode();
    }

    @Override
    public String toString() {
        return this.baseElement.toString();
    }
}
