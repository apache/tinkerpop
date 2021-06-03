package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class LimitStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * @type integer
     */
    public final Integer limit;
    
    /**
     * Constructs an immutable LimitStep object
     */
    public LimitStep(java.util.Optional<TraversalScope> scope, Integer limit) {
        this.scope = scope;
        this.limit = limit;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LimitStep)) {
            return false;
        }
        LimitStep o = (LimitStep) other;
        return scope.equals(o.scope)
            && limit.equals(o.limit);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode()
            + 3 * limit.hashCode();
    }
    
    /**
     * Construct a new immutable LimitStep object in which scope is overridden
     */
    public LimitStep withScope(java.util.Optional<TraversalScope> scope) {
        return new LimitStep(scope, limit);
    }
    
    /**
     * Construct a new immutable LimitStep object in which limit is overridden
     */
    public LimitStep withLimit(Integer limit) {
        return new LimitStep(scope, limit);
    }
}
