package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

/**
 * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
 */
public class Lt {
    public final GenericLiteral genericLiteral;
    
    /**
     * Constructs an immutable Lt object
     */
    public Lt(GenericLiteral genericLiteral) {
        this.genericLiteral = genericLiteral;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Lt)) {
            return false;
        }
        Lt o = (Lt) other;
        return genericLiteral.equals(o.genericLiteral);
    }
    
    @Override
    public int hashCode() {
        return 2 * genericLiteral.hashCode();
    }
}
