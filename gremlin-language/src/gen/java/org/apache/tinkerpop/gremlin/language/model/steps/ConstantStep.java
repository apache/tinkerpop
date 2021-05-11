package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

public class ConstantStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral e;
    
    /**
     * Constructs an immutable ConstantStep object
     */
    public ConstantStep(GenericLiteral e) {
        this.e = e;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ConstantStep)) return false;
        ConstantStep o = (ConstantStep) other;
        return e.equals(o.e);
    }
    
    @Override
    public int hashCode() {
        return 2 * e.hashCode();
    }
}
