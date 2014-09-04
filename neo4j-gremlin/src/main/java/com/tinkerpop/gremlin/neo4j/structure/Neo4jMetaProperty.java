package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Neo4jMetaProperty<V> implements MetaProperty<V> {

    public static final Label META_PROPERTY_LABEL = DynamicLabel.label(MetaProperty.LABEL);
    public static final String META_PROPERTY_PREFIX = "%$%";
    public static final String META_PROPERTY_KEY = "%$%key";

    private Node node;
    private final Neo4jVertex vertex;
    private final String key;
    private final V value;


    public Neo4jMetaProperty(final Neo4jVertex vertex, final String key, final V value) {
        this(vertex, null, key, value);
    }

    public Neo4jMetaProperty(final Neo4jVertex vertex, final Node node, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        this.node = node;
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
        this.vertex.graph.tx().readWrite();
        if (isNode()) {
            this.node.setProperty(key, value);
            return new Neo4jProperty<>(this, key, value);
        } else {
            this.node = this.vertex.graph.getBaseGraph().createNode(META_PROPERTY_LABEL);
            this.node.setProperty(META_PROPERTY_KEY, this.value);
            this.node.setProperty(key, value);
            final String prefixedKey = META_PROPERTY_PREFIX.concat(key);
            this.vertex.getBaseVertex().createRelationshipTo(this.node, DynamicRelationshipType.withName(prefixedKey));
            this.vertex.getBaseVertex().setProperty(prefixedKey, true);
            return new Neo4jProperty<>(this, key, value);
        }
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
        this.vertex.graph.tx().readWrite();
        if (isNode()) {
            final String prefixedKey = META_PROPERTY_PREFIX.concat(this.key);
            final PropertyContainer rawElement = this.vertex.getBaseElement();
            if (rawElement.hasProperty(prefixedKey)) {
                if (this.vertex.getBaseVertex().getDegree(DynamicRelationshipType.withName(prefixedKey), Direction.OUTGOING) == 1)
                    rawElement.removeProperty(prefixedKey);
            }
            this.node.getRelationships().forEach(Relationship::delete);
            this.node.delete();
        } else {
            final PropertyContainer rawElement = this.vertex.getBaseElement();
            if (rawElement.hasProperty(this.key)) {
                rawElement.removeProperty(this.key);
            }
        }
    }

    private boolean isNode() {
        return null != this.node;
    }

    protected void removeProperty(final Property property) {
        if (isNode() && this.node.hasProperty(property.key())) {
            this.node.removeProperty(property.key());
        }
    }

    @Override
    public MetaProperty.Iterators iterators() {
        return this.iterators;
    }

    private final MetaProperty.Iterators iterators = new Iterators(this);

    protected class Iterators implements MetaProperty.Iterators {

        private final Neo4jMetaProperty metaProperty;

        public Iterators(final Neo4jMetaProperty metaProperty) {
            this.metaProperty = metaProperty;
        }

        @Override
        public <U> Iterator<Property<U>> properties(String... propertyKeys) {
            if (!isNode()) return Collections.emptyIterator();
            else {
                vertex.graph.tx().readWrite();
                return (Iterator) StreamFactory.stream(node.getPropertyKeys())
                        .filter(key -> !key.equals(META_PROPERTY_KEY))
                        .filter(key -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, key) >= 0)
                        .filter(key -> !Graph.Key.isHidden(key))
                        .map(key -> new Neo4jProperty<>(metaProperty, key, (V) node.getProperty(key))).iterator();
            }
        }

        @Override
        public <U> Iterator<Property<U>> hiddens(String... propertyKeys) {
            if (!isNode()) return Collections.emptyIterator();
            else {
                vertex.graph.tx().readWrite();
                return (Iterator) StreamFactory.stream(node.getPropertyKeys())
                        .filter(key -> !key.equals(META_PROPERTY_KEY))
                        .filter(key -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, key) >= 0)
                        .filter(Graph.Key::isHidden)
                        .map(key -> new Neo4jProperty<>(metaProperty, key, (V) node.getProperty(key))).iterator();
            }
        }
    }
}
