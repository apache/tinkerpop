package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class OrderStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * Constructs an immutable OrderStep object
     */
    public OrderStep(java.util.Optional<TraversalScope> scope) {
        this.scope = scope;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof OrderStep)) return false;
        OrderStep o = (OrderStep) other;
        return scope.equals(o.scope);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode();
    }
}
