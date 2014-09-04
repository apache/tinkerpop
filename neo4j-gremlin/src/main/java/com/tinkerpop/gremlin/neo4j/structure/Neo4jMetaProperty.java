package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.neo4j.graphdb.PropertyContainer;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jMetaProperty<V> implements MetaProperty<V> {

    private final Neo4jVertex vertex;
    private final String key;
    private final V value;

    public Neo4jMetaProperty(final Neo4jVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }

    @Override
    public Vertex getElement() {
        return this.vertex;
    }

    @Override
    public Object id() {
        return this.key.hashCode() + this.value.hashCode();
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        throw new IllegalStateException("Multiproperties in Neo4j do not support properties");
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public void remove() {
        final PropertyContainer rawElement = this.vertex.getBaseElement();
        if (rawElement.hasProperty(this.key)) {
            this.vertex.graph.tx().readWrite();
            rawElement.removeProperty(this.key);
        }
    }

    @Override
    public MetaProperty.Iterators iterators() {
        return Neo4jMetaProperty.ITERATORS;
    }

    private static final MetaProperty.Iterators ITERATORS = new Iterators();

    protected static class Iterators implements MetaProperty.Iterators {

        @Override
        public <U> Iterator<Property<U>> properties(String... propertyKeys) {
            throw new IllegalStateException("Multiproperties in Neo4j do not support properties");
        }

        @Override
        public <U> Iterator<Property<U>> hiddens(String... propertyKeys) {
            throw new IllegalStateException("Multiproperties in Neo4j do not support properties");
        }
    }
}
