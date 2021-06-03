package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;
import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalCardinality;

public class PropertyStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalCardinality
     */
    public final java.util.Optional<TraversalCardinality> cardinality;
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral key;
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral value;
    
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final java.util.List<GenericLiteral> keyValues;
    
    /**
     * Constructs an immutable PropertyStep object
     */
    public PropertyStep(java.util.Optional<TraversalCardinality> cardinality, GenericLiteral key, GenericLiteral value, java.util.List<GenericLiteral> keyValues) {
        this.cardinality = cardinality;
        this.key = key;
        this.value = value;
        this.keyValues = keyValues;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PropertyStep)) {
            return false;
        }
        PropertyStep o = (PropertyStep) other;
        return cardinality.equals(o.cardinality)
            && key.equals(o.key)
            && value.equals(o.value)
            && keyValues.equals(o.keyValues);
    }
    
    @Override
    public int hashCode() {
        return 2 * cardinality.hashCode()
            + 3 * key.hashCode()
            + 5 * value.hashCode()
            + 7 * keyValues.hashCode();
    }
    
    /**
     * Construct a new immutable PropertyStep object in which cardinality is overridden
     */
    public PropertyStep withCardinality(java.util.Optional<TraversalCardinality> cardinality) {
        return new PropertyStep(cardinality, key, value, keyValues);
    }
    
    /**
     * Construct a new immutable PropertyStep object in which key is overridden
     */
    public PropertyStep withKey(GenericLiteral key) {
        return new PropertyStep(cardinality, key, value, keyValues);
    }
    
    /**
     * Construct a new immutable PropertyStep object in which value is overridden
     */
    public PropertyStep withValue(GenericLiteral value) {
        return new PropertyStep(cardinality, key, value, keyValues);
    }
    
    /**
     * Construct a new immutable PropertyStep object in which keyValues is overridden
     */
    public PropertyStep withKeyValues(java.util.List<GenericLiteral> keyValues) {
        return new PropertyStep(cardinality, key, value, keyValues);
    }
}
