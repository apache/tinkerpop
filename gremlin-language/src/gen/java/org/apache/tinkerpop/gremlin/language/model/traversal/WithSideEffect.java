package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;
import org.example.org.apache.tinkerpop.gremlin.language.model.literals.StringLiteral;

public class WithSideEffect {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.StringLiteral
     */
    public final StringLiteral key;
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral initialValue;
    
    /**
     * Constructs an immutable WithSideEffect object
     */
    public WithSideEffect(StringLiteral key, GenericLiteral initialValue) {
        this.key = key;
        this.initialValue = initialValue;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof WithSideEffect)) {
            return false;
        }
        WithSideEffect o = (WithSideEffect) other;
        return key.equals(o.key)
            && initialValue.equals(o.initialValue);
    }
    
    @Override
    public int hashCode() {
        return 2 * key.hashCode()
            + 3 * initialValue.hashCode();
    }
    
    /**
     * Construct a new immutable WithSideEffect object in which key is overridden
     */
    public WithSideEffect withKey(StringLiteral key) {
        return new WithSideEffect(key, initialValue);
    }
    
    /**
     * Construct a new immutable WithSideEffect object in which initialValue is overridden
     */
    public WithSideEffect withInitialValue(GenericLiteral initialValue) {
        return new WithSideEffect(key, initialValue);
    }
}
