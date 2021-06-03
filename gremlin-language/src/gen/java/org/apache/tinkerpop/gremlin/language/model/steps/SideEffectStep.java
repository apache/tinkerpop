package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class SideEffectStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final NestedTraversal sideEffectTraversal;
    
    /**
     * Constructs an immutable SideEffectStep object
     */
    public SideEffectStep(NestedTraversal sideEffectTraversal) {
        this.sideEffectTraversal = sideEffectTraversal;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SideEffectStep)) {
            return false;
        }
        SideEffectStep o = (SideEffectStep) other;
        return sideEffectTraversal.equals(o.sideEffectTraversal);
    }
    
    @Override
    public int hashCode() {
        return 2 * sideEffectTraversal.hashCode();
    }
}
