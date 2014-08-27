package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;
import org.javatuples.Pair;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class Neo4jElement implements Element, WrappedElement<PropertyContainer> {
    protected final Neo4jGraph graph;
    protected PropertyContainer baseElement;

    public Neo4jElement(final Neo4jGraph graph) {
        this.graph = graph;
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
    public String label() {
        this.graph.tx().readWrite();
        if (this.baseElement instanceof Node)
            return ((Node) this.baseElement).getLabels().iterator().next().name();
        else
            return ((Relationship) this.baseElement).getType().name();
    }

    @Override
    public Map<String, Property> properties() {
        this.graph.tx().readWrite();
        return keys().stream()
                .map(key -> Pair.<String, Property>with(key, new Neo4jProperty<>(this, key, this.baseElement.getProperty(key))))
                .collect(Collectors.toMap(kv -> kv.getValue0(), kv -> kv.getValue1()));
    }

    @Override
    public Map<String, Property> hiddens() {
        this.graph.tx().readWrite();
        return hiddenKeys().stream()
                .map(key -> Pair.<String, Property>with(key, new Neo4jProperty<>(this, key, this.baseElement.getProperty(Graph.Key.hide(key)))))
                .collect(Collectors.toMap(kv -> kv.getValue0(), kv -> kv.getValue1()));
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

        if (this.baseElement.hasProperty(key))
            return new Neo4jProperty<>(this, key, (V) this.baseElement.getProperty(key));
        else
            return Property.empty();
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

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.id().hashCode();
    }

    public PropertyContainer getBaseElement() {
        return this.baseElement;
    }
}
