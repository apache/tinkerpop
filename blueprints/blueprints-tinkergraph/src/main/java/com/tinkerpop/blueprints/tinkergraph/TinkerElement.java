package com.tinkerpop.blueprints.tinkergraph;


import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.computer.GraphComputer;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
abstract class TinkerElement implements Element, Serializable {

    protected Map<String, Property> properties = new HashMap<>();
    protected Map<String, Object> annotations = new HashMap<>();
    protected final String id;
    protected final String label;
    protected final TinkerGraph graph;

    protected String centricId;
    protected TinkerGraphComputer.State state = TinkerGraphComputer.State.STANDARD;
    protected TinkerAnnotationMemory annotationMemory;


    protected TinkerElement(final String id, final String label, final TinkerGraph graph) {
        this.graph = graph;
        this.id = id;
        this.label = label;
    }

    public int hashCode() {
        return this.id.hashCode();
    }

    public String getId() {
        return this.id;
    }

    public String getLabel() {
        return this.label;
    }

    public Map<String, Property> getProperties() {
        return new HashMap<>(this.properties);
    }

    public <V> Property<V> getProperty(final String key) {
        if (TinkerGraphComputer.State.STANDARD == this.state) {
            return this.properties.getOrDefault(key, Property.empty());
        } else if (TinkerGraphComputer.State.CENTRIC == this.state) {
            final Property property = this.properties.getOrDefault(key, Property.empty());
            return property.isPresent() ?
                    ((TinkerProperty) property).createClone(TinkerGraphComputer.State.CENTRIC, this.annotationMemory) :
                    property;
        } else {
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeRead();
        }
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public <V> void setAnnotation(final String key, final V value) {
        if (this.state == TinkerGraphComputer.State.STANDARD) {
            this.annotations.put(key, value);
        } else if (this.state == TinkerGraphComputer.State.CENTRIC) {
            if (this.annotationMemory.isComputeKey(key))
                this.annotationMemory.setAnnotation(this, key, value);
            else
                throw GraphComputer.Exceptions.providedKeyIsNotAComputeKey(key);
        } else {
            throw GraphComputer.Exceptions.adjacentVertexAnnotationsCanNotBeWritten();
        }
    }

    public <V> Optional<V> getAnnotation(final String key) {
        if (this.state == TinkerGraphComputer.State.STANDARD) {
            return Optional.ofNullable((V) this.annotations.get(key));
        } else if (this.state == TinkerGraphComputer.State.CENTRIC) {
            if (this.annotationMemory.isComputeKey(key))
                return this.annotationMemory.getAnnotation(this, key);
            else
                return Optional.ofNullable((V) this.annotations.get(key));
        } else {
            throw GraphComputer.Exceptions.adjacentVertexAnnotationsCanNotBeRead();
        }
    }

    public abstract void remove();
}
