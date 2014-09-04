package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.MetaProperty;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
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
    public static final String META_PROPERTY_VALUE = "%$%value";
    public static final String META_PROPERTY_TOKEN = "%$%metaProperty";

    private Node node;
    private final Neo4jVertex vertex;
    private final String key;
    private final V value;


    public Neo4jMetaProperty(final Neo4jVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        this.node = null;
    }

    public Neo4jMetaProperty(final Neo4jVertex vertex, final Node node) {
        this.vertex = vertex;
        this.node = node;
        this.key = (String) node.getProperty(META_PROPERTY_KEY);
        this.value = (V) node.getProperty(META_PROPERTY_VALUE);
    }

    @Override
    public Vertex getElement() {
        return this.vertex;
    }

    @Override
    public Object id() {
        return this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual((MetaProperty) this, object);
    }

    @Override
    public int hashCode() {
        return this.key.hashCode() + this.value.hashCode() + this.vertex.hashCode();
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        this.vertex.graph.tx().readWrite();
        if (isNode()) {
            if (key.equals(META_PROPERTY_KEY) || key.equals(META_PROPERTY_VALUE))
                throw new IllegalArgumentException("The following key is a reserved key for Neo4jMetaProperty: " + key);
            this.node.setProperty(key, value);
            return new Neo4jProperty<>(this, key, value);
        } else {
            this.node = this.vertex.graph.getBaseGraph().createNode(META_PROPERTY_LABEL);
            this.node.setProperty(META_PROPERTY_KEY, this.key);
            this.node.setProperty(META_PROPERTY_VALUE, this.value);
            this.node.setProperty(key, value);
            this.vertex.getBaseVertex().createRelationshipTo(this.node, DynamicRelationshipType.withName(META_PROPERTY_PREFIX.concat(this.key)));
            this.vertex.getBaseVertex().setProperty(this.key, META_PROPERTY_TOKEN);
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
            if (this.vertex.getBaseVertex().hasProperty(this.key)) {
                if (this.vertex.getBaseVertex().getDegree(DynamicRelationshipType.withName(META_PROPERTY_PREFIX.concat(this.key)), Direction.OUTGOING) == 1)
                    this.vertex.getBaseVertex().removeProperty(this.key);
            }
            this.node.getRelationships().forEach(Relationship::delete);
            this.node.delete();
        } else {
            if (this.vertex.getBaseVertex().hasProperty(this.key)) {
                this.vertex.getBaseVertex().removeProperty(this.key);
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
                        .filter(key -> !key.equals(META_PROPERTY_VALUE))
                        .filter(key -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, key) >= 0)
                        .filter(key -> !Graph.Key.isHidden(key))
                        .map(key -> new Neo4jProperty<>(this.metaProperty, key, (V) node.getProperty(key))).iterator();
            }
        }

        @Override
        public <U> Iterator<Property<U>> hiddens(String... propertyKeys) {
            if (!isNode()) return Collections.emptyIterator();
            else {
                vertex.graph.tx().readWrite();
                return (Iterator) StreamFactory.stream(node.getPropertyKeys())
                        .filter(key -> !key.equals(META_PROPERTY_KEY))
                        .filter(key -> !key.equals(META_PROPERTY_VALUE))
                        .filter(key -> propertyKeys.length == 0 || Arrays.binarySearch(propertyKeys, key) >= 0)
                        .filter(Graph.Key::isHidden)
                        .map(key -> new Neo4jProperty<>(this.metaProperty, key, (V) node.getProperty(key))).iterator();
            }
        }
    }
}
