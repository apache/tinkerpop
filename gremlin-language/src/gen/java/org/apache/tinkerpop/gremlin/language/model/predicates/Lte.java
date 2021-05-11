package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

/**
 * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
 */
public class Lte {
    public final GenericLiteral genericLiteral;
    
    /**
     * Constructs an immutable Lte object
     */
    public Lte(GenericLiteral genericLiteral) {
        this.genericLiteral = genericLiteral;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Lte)) return false;
        Lte o = (Lte) other;
        return genericLiteral.equals(o.genericLiteral);
    }
    
    @Override
    public int hashCode() {
        return 2 * genericLiteral.hashCode();
    }
}
