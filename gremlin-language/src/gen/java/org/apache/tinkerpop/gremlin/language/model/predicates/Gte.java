package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

/**
 * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
 */
public class Gte {
    public final GenericLiteral genericLiteral;
    
    /**
     * Constructs an immutable Gte object
     */
    public Gte(GenericLiteral genericLiteral) {
        this.genericLiteral = genericLiteral;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Gte)) return false;
        Gte o = (Gte) other;
        return genericLiteral.equals(o.genericLiteral);
    }
    
    @Override
    public int hashCode() {
        return 2 * genericLiteral.hashCode();
    }
}
