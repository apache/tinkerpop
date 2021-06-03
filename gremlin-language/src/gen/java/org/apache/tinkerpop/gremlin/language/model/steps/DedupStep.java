package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class DedupStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * @type list: string
     */
    public final java.util.List<String> dedupLabels;
    
    /**
     * Constructs an immutable DedupStep object
     */
    public DedupStep(java.util.Optional<TraversalScope> scope, java.util.List<String> dedupLabels) {
        this.scope = scope;
        this.dedupLabels = dedupLabels;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DedupStep)) {
            return false;
        }
        DedupStep o = (DedupStep) other;
        return scope.equals(o.scope)
            && dedupLabels.equals(o.dedupLabels);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode()
            + 3 * dedupLabels.hashCode();
    }
    
    /**
     * Construct a new immutable DedupStep object in which scope is overridden
     */
    public DedupStep withScope(java.util.Optional<TraversalScope> scope) {
        return new DedupStep(scope, dedupLabels);
    }
    
    /**
     * Construct a new immutable DedupStep object in which dedupLabels is overridden
     */
    public DedupStep withDedupLabels(java.util.List<String> dedupLabels) {
        return new DedupStep(scope, dedupLabels);
    }
}
