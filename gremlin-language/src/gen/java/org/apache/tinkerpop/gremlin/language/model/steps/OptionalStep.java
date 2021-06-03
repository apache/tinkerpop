package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class OptionalStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final NestedTraversal optionalTraversal;
    
    /**
     * Constructs an immutable OptionalStep object
     */
    public OptionalStep(NestedTraversal optionalTraversal) {
        this.optionalTraversal = optionalTraversal;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof OptionalStep)) {
            return false;
        }
        OptionalStep o = (OptionalStep) other;
        return optionalTraversal.equals(o.optionalTraversal);
    }
    
    @Override
    public int hashCode() {
        return 2 * optionalTraversal.hashCode();
    }
}
