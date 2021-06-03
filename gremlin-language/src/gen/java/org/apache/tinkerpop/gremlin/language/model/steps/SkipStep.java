package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class SkipStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * @type integer
     */
    public final Integer skip;
    
    /**
     * Constructs an immutable SkipStep object
     */
    public SkipStep(java.util.Optional<TraversalScope> scope, Integer skip) {
        this.scope = scope;
        this.skip = skip;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SkipStep)) {
            return false;
        }
        SkipStep o = (SkipStep) other;
        return scope.equals(o.scope)
            && skip.equals(o.skip);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode()
            + 3 * skip.hashCode();
    }
    
    /**
     * Construct a new immutable SkipStep object in which scope is overridden
     */
    public SkipStep withScope(java.util.Optional<TraversalScope> scope) {
        return new SkipStep(scope, skip);
    }
    
    /**
     * Construct a new immutable SkipStep object in which skip is overridden
     */
    public SkipStep withSkip(Integer skip) {
        return new SkipStep(scope, skip);
    }
}
