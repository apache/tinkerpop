package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

/**
 * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
 */
public class Gt {
    public final GenericLiteral genericLiteral;
    
    /**
     * Constructs an immutable Gt object
     */
    public Gt(GenericLiteral genericLiteral) {
        this.genericLiteral = genericLiteral;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Gt)) {
            return false;
        }
        Gt o = (Gt) other;
        return genericLiteral.equals(o.genericLiteral);
    }
    
    @Override
    public int hashCode() {
        return 2 * genericLiteral.hashCode();
    }
}
