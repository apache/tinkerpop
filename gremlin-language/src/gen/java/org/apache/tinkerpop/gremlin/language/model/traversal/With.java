package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

public class With {
    /**
     * @type string
     */
    public final String key;
    
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final java.util.Optional<GenericLiteral> value;
    
    /**
     * Constructs an immutable With object
     */
    public With(String key, java.util.Optional<GenericLiteral> value) {
        this.key = key;
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof With)) {
            return false;
        }
        With o = (With) other;
        return key.equals(o.key)
            && value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * key.hashCode()
            + 3 * value.hashCode();
    }
    
    /**
     * Construct a new immutable With object in which key is overridden
     */
    public With withKey(String key) {
        return new With(key, value);
    }
    
    /**
     * Construct a new immutable With object in which value is overridden
     */
    public With withValue(java.util.Optional<GenericLiteral> value) {
        return new With(key, value);
    }
}
