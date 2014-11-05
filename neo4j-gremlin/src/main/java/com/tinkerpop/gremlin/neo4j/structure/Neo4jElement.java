package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class Neo4jElement implements Element, Element.Iterators, WrappedElement<PropertyContainer> {
    protected final Neo4jGraph graph;
    protected PropertyContainer baseElement;
    protected boolean removed = false;

    public Neo4jElement(final Neo4jGraph graph) {
        this.graph = graph;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Object id() {
        this.graph.tx().readWrite();
        if (this.baseElement instanceof Node)
            return ((Node) this.baseElement).getId();
        else
            return ((Relationship) this.baseElement).getId();
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.baseElement.getPropertyKeys()) {
            if (!Graph.Key.isHidden(key))
                keys.add(key);
        }
        return keys;
    }

    @Override
    public Set<String> hiddenKeys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.baseElement.getPropertyKeys()) {
            if (Graph.Key.isHidden(key))
                keys.add(Graph.Key.unHide(key));
        }
        return keys;
    }

    @Override
    public <V> Property<V> property(final String key) {
        this.graph.tx().readWrite();
        try {
            if (this.baseElement.hasProperty(key))
                return new Neo4jProperty<>(this, key, (V) this.baseElement.getProperty(key));
            else
                return Property.empty();
        } catch (IllegalStateException ise) {
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
        } catch (IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return this.id().hashCode();
    }

    @Override
    public PropertyContainer getBaseElement() {
        return this.baseElement;
    }

    @Override
    public <V> Iterator<? extends Property<V>> propertyIterator(final String... propertyKeys) {
        graph.tx().readWrite();
        return StreamFactory.stream(baseElement.getPropertyKeys())
                .filter(key -> propertyKeys.length == 0 || Stream.of(propertyKeys).filter(k -> k.equals(key)).findAny().isPresent())
                .filter(key -> !Graph.Key.isHidden(key))
                .map(key -> new Neo4jProperty<>(Neo4jElement.this, key, (V) baseElement.getProperty(key))).iterator();
    }

    @Override
    public <V> Iterator<? extends Property<V>> hiddenPropertyIterator(final String... propertyKeys) {
        graph.tx().readWrite();
        return StreamFactory.stream(baseElement.getPropertyKeys())
                .filter(Graph.Key::isHidden)
                .filter(key -> propertyKeys.length == 0 || Stream.of(propertyKeys).filter(k -> k.equals(Graph.Key.unHide(key))).findAny().isPresent())
                .map(key -> new Neo4jProperty<>(Neo4jElement.this, key, (V) baseElement.getProperty(key))).iterator();
    }
}
