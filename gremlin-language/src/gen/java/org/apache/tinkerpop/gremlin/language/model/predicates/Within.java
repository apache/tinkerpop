package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

/**
 * @type list: org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
 */
public class Within {
    public final java.util.List<GenericLiteral> value;
    
    /**
     * Constructs an immutable Within object
     */
    public Within(java.util.List<GenericLiteral> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Within)) {
            return false;
        }
        Within o = (Within) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
