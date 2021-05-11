package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalBiFunction;

public class SackStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalBiFunction
     */
    public final java.util.Optional<TraversalBiFunction> sackOperator;
    
    /**
     * Constructs an immutable SackStep object
     */
    public SackStep(java.util.Optional<TraversalBiFunction> sackOperator) {
        this.sackOperator = sackOperator;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SackStep)) return false;
        SackStep o = (SackStep) other;
        return sackOperator.equals(o.sackOperator);
    }
    
    @Override
    public int hashCode() {
        return 2 * sackOperator.hashCode();
    }
}
