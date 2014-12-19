package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class Neo4jElement implements Element, Element.Iterators, WrappedElement<PropertyContainer> {
    protected final Neo4jGraph graph;
    protected final PropertyContainer baseElement;
    protected boolean removed = false;

    public Neo4jElement(final PropertyContainer baseElement, final Neo4jGraph graph) {
        this.baseElement = baseElement;
        this.graph = graph;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Object id() {
        this.graph.tx().readWrite();
        return this.baseElement instanceof Node ? ((Node) this.baseElement).getId() : ((Relationship) this.baseElement).getId();
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        return Element.super.keys();
    }

    @Override
    public <V> Property<V> property(final String key) {
        this.graph.tx().readWrite();
        try {
            if (this.baseElement.hasProperty(key))
                return new Neo4jProperty<>(this, key, (V) this.baseElement.getProperty(key));
            else
                return Property.empty();
        } catch (final IllegalStateException e) {
            throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id());
        }
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();

        try {
            this.baseElement.setProperty(key, value);
            return new Neo4jProperty<>(this, key, value);
        } catch (final IllegalArgumentException e) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public PropertyContainer getBaseElement() {
        return this.baseElement;
    }

    @Override
    public <V> Iterator<? extends Property<V>> propertyIterator(final String... propertyKeys) {
        this.graph.tx().readWrite();
        return IteratorUtils.map(IteratorUtils.filter(this.baseElement.getPropertyKeys().iterator(), key -> ElementHelper.keyExists(key, propertyKeys)), key -> new Neo4jProperty<>(this, key, (V) this.baseElement.getProperty(key)));
    }

}
