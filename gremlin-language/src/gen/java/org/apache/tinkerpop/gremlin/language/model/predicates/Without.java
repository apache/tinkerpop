package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

/**
 * @type list: org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
 */
public class Without {
    public final java.util.List<GenericLiteral> value;
    
    /**
     * Constructs an immutable Without object
     */
    public Without(java.util.List<GenericLiteral> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Without)) return false;
        Without o = (Without) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
