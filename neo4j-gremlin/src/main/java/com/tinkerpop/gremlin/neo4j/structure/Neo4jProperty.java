package com.tinkerpop.gremlin.neo4j.structure;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Neo4jProperty<V> implements Property<V> {

    private final Element element;
    private final String key;
    private final Neo4jGraph graph;
    private V value;

    public Neo4jProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.key = key;
        this.value = value;
        this.graph = element instanceof Neo4jVertexProperty ?
                ((Neo4jVertex) (((Neo4jVertexProperty) element).element())).graph :
                ((Neo4jElement) element).graph;
    }

    @Override
    public <E extends Element> E element() {
        return (E) this.element;
    }

    @Override
    public String key() {
        return Graph.Key.unHide(this.key);
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public boolean isHidden() {
        return Graph.Key.isHidden(this.key);
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return this.key.hashCode() + this.value.hashCode() + this.element.hashCode();
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        if (this.element instanceof VertexProperty) {
            final Node node = ((Neo4jVertexProperty) this.element).getBaseVertex();
            if (null != node && node.hasProperty(this.key)) {
                node.removeProperty(this.key);
            }
        } else {
            final PropertyContainer propertyContainer = ((Neo4jElement) this.element).getBaseElement();
            if (propertyContainer.hasProperty(this.key)) {
                propertyContainer.removeProperty(this.key);
            }
        }
    }

}