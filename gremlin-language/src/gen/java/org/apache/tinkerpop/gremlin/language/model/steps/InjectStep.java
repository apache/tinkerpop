package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

public class InjectStep {
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final java.util.List<GenericLiteral> injections;
    
    /**
     * Constructs an immutable InjectStep object
     */
    public InjectStep(java.util.List<GenericLiteral> injections) {
        this.injections = injections;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof InjectStep)) {
            return false;
        }
        InjectStep o = (InjectStep) other;
        return injections.equals(o.injections);
    }
    
    @Override
    public int hashCode() {
        return 2 * injections.hashCode();
    }
}
