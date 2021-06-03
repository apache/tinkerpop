package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class OrStep {
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final java.util.List<NestedTraversal> orTraversals;
    
    /**
     * Constructs an immutable OrStep object
     */
    public OrStep(java.util.List<NestedTraversal> orTraversals) {
        this.orTraversals = orTraversals;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof OrStep)) {
            return false;
        }
        OrStep o = (OrStep) other;
        return orTraversals.equals(o.orTraversals);
    }
    
    @Override
    public int hashCode() {
        return 2 * orTraversals.hashCode();
    }
}
