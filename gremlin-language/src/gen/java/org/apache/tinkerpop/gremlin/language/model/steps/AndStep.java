package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class AndStep {
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final java.util.List<NestedTraversal> andTraversals;
    
    /**
     * Constructs an immutable AndStep object
     */
    public AndStep(java.util.List<NestedTraversal> andTraversals) {
        this.andTraversals = andTraversals;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AndStep)) {
            return false;
        }
        AndStep o = (AndStep) other;
        return andTraversals.equals(o.andTraversals);
    }
    
    @Override
    public int hashCode() {
        return 2 * andTraversals.hashCode();
    }
}
