package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class MinStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * Constructs an immutable MinStep object
     */
    public MinStep(java.util.Optional<TraversalScope> scope) {
        this.scope = scope;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MinStep)) return false;
        MinStep o = (MinStep) other;
        return scope.equals(o.scope);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode();
    }
}
