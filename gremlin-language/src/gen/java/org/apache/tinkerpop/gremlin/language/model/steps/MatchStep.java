package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class MatchStep {
    /**
     * @type list: org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final java.util.List<NestedTraversal> matchTraversals;
    
    /**
     * Constructs an immutable MatchStep object
     */
    public MatchStep(java.util.List<NestedTraversal> matchTraversals) {
        this.matchTraversals = matchTraversals;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MatchStep)) return false;
        MatchStep o = (MatchStep) other;
        return matchTraversals.equals(o.matchTraversals);
    }
    
    @Override
    public int hashCode() {
        return 2 * matchTraversals.hashCode();
    }
}
