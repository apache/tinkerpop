package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class TailStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * @type optional: integer
     */
    public final java.util.Optional<Integer> limit;
    
    /**
     * Constructs an immutable TailStep object
     */
    public TailStep(java.util.Optional<TraversalScope> scope, java.util.Optional<Integer> limit) {
        this.scope = scope;
        this.limit = limit;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TailStep)) return false;
        TailStep o = (TailStep) other;
        return scope.equals(o.scope)
            && limit.equals(o.limit);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode()
            + 3 * limit.hashCode();
    }
    
    /**
     * Construct a new immutable TailStep object in which scope is overridden
     */
    public TailStep withScope(java.util.Optional<TraversalScope> scope) {
        return new TailStep(scope, limit);
    }
    
    /**
     * Construct a new immutable TailStep object in which limit is overridden
     */
    public TailStep withLimit(java.util.Optional<Integer> limit) {
        return new TailStep(scope, limit);
    }
}
