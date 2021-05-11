package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class LocalStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final NestedTraversal localTraversal;
    
    /**
     * Constructs an immutable LocalStep object
     */
    public LocalStep(NestedTraversal localTraversal) {
        this.localTraversal = localTraversal;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LocalStep)) return false;
        LocalStep o = (LocalStep) other;
        return localTraversal.equals(o.localTraversal);
    }
    
    @Override
    public int hashCode() {
        return 2 * localTraversal.hashCode();
    }
}
