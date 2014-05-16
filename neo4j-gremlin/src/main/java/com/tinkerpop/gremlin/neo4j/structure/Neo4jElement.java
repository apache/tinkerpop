package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
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
abstract class Neo4jElement implements Element {
    protected final Neo4jGraph graph;
    protected PropertyContainer rawElement;

    public Neo4jElement(final Neo4jGraph graph) {
        this.graph = graph;
    }

    @Override
    public Object id() {
        this.graph.tx().readWrite();
        if (this.rawElement instanceof Node)
            return ((Node) this.rawElement).getId();
        else
            return ((Relationship) this.rawElement).getId();
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        // todo: what to do when there are multiple labels on a vertex!!! harden the approach below
        if (this.rawElement instanceof Node)
            return ((Node) this.rawElement).getLabels().iterator().next().name();
        else
            return ((Relationship) this.rawElement).getType().name();
    }

    @Override
    public Map<String, Property> properties() {
        this.graph.tx().readWrite();
        return keys().stream()
                .filter(key -> !key.startsWith(Graph.HIDDEN_PREFIX))
                .map(key -> Pair.<String, Property>with(key, new Neo4jProperty<>(this, key, this.rawElement.getProperty(key))))
                .collect(Collectors.toMap(kv -> kv.getValue0(), kv -> kv.getValue1()));
    }

    @Override
    public Map<String, Property> hiddens() {
        this.graph.tx().readWrite();
        return keys().stream()
                .filter(key -> key.startsWith(Graph.HIDDEN_PREFIX))
                .map(key -> Pair.<String, Property>with(key, new Neo4jProperty<>(this, key, this.rawElement.getProperty(key))))
                .collect(Collectors.toMap(kv -> kv.getValue0(), kv -> kv.getValue1()));
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.rawElement.getPropertyKeys()) {
            if (!key.startsWith(Graph.HIDDEN_PREFIX))
                keys.add(key);
        }
        return keys;
    }

    @Override
    public Set<String> hiddenKeys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.rawElement.getPropertyKeys()) {
            if (key.startsWith(Graph.HIDDEN_PREFIX))
                keys.add(ElementHelper.removeHiddenPrefix(key));
        }
        return keys;
    }

    @Override
    public <V> Property<V> property(final String key) {
        this.graph.tx().readWrite();

        // todo: do we stil convert collection down to array?? - return (T) tryConvertCollectionToArrayList(this.rawElement.property(key));
        if (this.rawElement.hasProperty(key))
            return new Neo4jProperty<>(this, key, (V) this.rawElement.getProperty(key));
        else
            return Property.empty();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();

        // todo: deal with annotatedlist/value
        // todo: do we worry about - this.rawElement.setProperty(key, tryConvertCollectionToArray(value));

        try {
            this.rawElement.setProperty(key, value);
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

    public PropertyContainer getRawElement() {
        return this.rawElement;
    }
}
