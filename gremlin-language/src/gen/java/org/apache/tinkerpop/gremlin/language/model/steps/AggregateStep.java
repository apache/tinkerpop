package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class AggregateStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * @type string
     */
    public final String sideEffectKey;
    
    /**
     * Constructs an immutable AggregateStep object
     */
    public AggregateStep(java.util.Optional<TraversalScope> scope, String sideEffectKey) {
        this.scope = scope;
        this.sideEffectKey = sideEffectKey;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof AggregateStep)) {
            return false;
        }
        AggregateStep o = (AggregateStep) other;
        return scope.equals(o.scope)
            && sideEffectKey.equals(o.sideEffectKey);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode()
            + 3 * sideEffectKey.hashCode();
    }
    
    /**
     * Construct a new immutable AggregateStep object in which scope is overridden
     */
    public AggregateStep withScope(java.util.Optional<TraversalScope> scope) {
        return new AggregateStep(scope, sideEffectKey);
    }
    
    /**
     * Construct a new immutable AggregateStep object in which sideEffectKey is overridden
     */
    public AggregateStep withSideEffectKey(String sideEffectKey) {
        return new AggregateStep(scope, sideEffectKey);
    }
}
