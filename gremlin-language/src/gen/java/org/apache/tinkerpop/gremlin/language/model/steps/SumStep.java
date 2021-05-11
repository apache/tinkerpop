package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class SumStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * Constructs an immutable SumStep object
     */
    public SumStep(java.util.Optional<TraversalScope> scope) {
        this.scope = scope;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SumStep)) return false;
        SumStep o = (SumStep) other;
        return scope.equals(o.scope);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode();
    }
}
