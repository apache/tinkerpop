package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class CoalesceStep {
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final java.util.List<NestedTraversal> coalesceTraversals;
    
    /**
     * Constructs an immutable CoalesceStep object
     */
    public CoalesceStep(java.util.List<NestedTraversal> coalesceTraversals) {
        this.coalesceTraversals = coalesceTraversals;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CoalesceStep)) {
            return false;
        }
        CoalesceStep o = (CoalesceStep) other;
        return coalesceTraversals.equals(o.coalesceTraversals);
    }
    
    @Override
    public int hashCode() {
        return 2 * coalesceTraversals.hashCode();
    }
}
