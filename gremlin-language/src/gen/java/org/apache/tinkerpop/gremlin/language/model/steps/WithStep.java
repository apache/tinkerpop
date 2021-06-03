package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

public class WithStep {
    /**
     * @type string
     */
    public final String key;
    
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final java.util.Optional<GenericLiteral> value;
    
    /**
     * Constructs an immutable WithStep object
     */
    public WithStep(String key, java.util.Optional<GenericLiteral> value) {
        this.key = key;
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof WithStep)) {
            return false;
        }
        WithStep o = (WithStep) other;
        return key.equals(o.key)
            && value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * key.hashCode()
            + 3 * value.hashCode();
    }
    
    /**
     * Construct a new immutable WithStep object in which key is overridden
     */
    public WithStep withKey(String key) {
        return new WithStep(key, value);
    }
    
    /**
     * Construct a new immutable WithStep object in which value is overridden
     */
    public WithStep withValue(java.util.Optional<GenericLiteral> value) {
        return new WithStep(key, value);
    }
}
