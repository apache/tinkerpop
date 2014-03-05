package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Element;
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
    public Object getId() {
        this.graph.tx().readWrite();
        if (this.rawElement instanceof Node)
            return ((Node) this.rawElement).getId();
        else
            return ((Relationship) this.rawElement).getId();
    }

    @Override
    public String getLabel() {
        // todo: what to do when there are multiple labels on a vertex!!! harden the approach below
        if (this.rawElement instanceof Node)
            return ((Node) this.rawElement).getLabels().iterator().next().name();
        else
            return ((Relationship) this.rawElement).getType().name();
    }

    @Override
    public Map<String, Property> getProperties() {
        this.graph.tx().readWrite();
        return getPropertyKeys().stream()
                .map(key -> Pair.<String, Property>with(key, new Neo4jProperty<>(this, key, this.rawElement.getProperty(key))))
                .collect(Collectors.toMap(kv -> kv.getValue0(), kv -> kv.getValue1()));
    }

    @Override
    public Set<String> getPropertyKeys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.rawElement.getPropertyKeys()) {
            keys.add(key);
        }
        return keys;
    }

    @Override
    public <V> Property<V> getProperty(final String key) {
        this.graph.tx().readWrite();

        // todo: do we stil convert collection down to array?? - return (T) tryConvertCollectionToArrayList(this.rawElement.getProperty(key));
        if (this.rawElement.hasProperty(key))
            return new Neo4jProperty<>(this, key, (V) this.rawElement.getProperty(key));
        else
            return Property.empty();
    }

    @Override
    public <V> void setProperty(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();

        // todo: deal with annotatedlist/value
        // todo: do we worry about - this.rawElement.setProperty(key, tryConvertCollectionToArray(value));

        this.rawElement.setProperty(key, value);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.getId().hashCode();
    }

    public PropertyContainer getRawElement() {
        return this.rawElement;
    }
}
