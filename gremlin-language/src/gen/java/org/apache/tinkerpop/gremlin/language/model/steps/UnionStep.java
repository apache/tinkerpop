package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class UnionStep {
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final java.util.List<NestedTraversal> unionTraversals;
    
    /**
     * Constructs an immutable UnionStep object
     */
    public UnionStep(java.util.List<NestedTraversal> unionTraversals) {
        this.unionTraversals = unionTraversals;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof UnionStep)) {
            return false;
        }
        UnionStep o = (UnionStep) other;
        return unionTraversals.equals(o.unionTraversals);
    }
    
    @Override
    public int hashCode() {
        return 2 * unionTraversals.hashCode();
    }
}
