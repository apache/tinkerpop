package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class RangeStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * @type integer
     */
    public final Integer low;
    
    /**
     * @type integer
     */
    public final Integer high;
    
    /**
     * Constructs an immutable RangeStep object
     */
    public RangeStep(java.util.Optional<TraversalScope> scope, Integer low, Integer high) {
        this.scope = scope;
        this.low = low;
        this.high = high;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RangeStep)) return false;
        RangeStep o = (RangeStep) other;
        return scope.equals(o.scope)
            && low.equals(o.low)
            && high.equals(o.high);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode()
            + 3 * low.hashCode()
            + 5 * high.hashCode();
    }
    
    /**
     * Construct a new immutable RangeStep object in which scope is overridden
     */
    public RangeStep withScope(java.util.Optional<TraversalScope> scope) {
        return new RangeStep(scope, low, high);
    }
    
    /**
     * Construct a new immutable RangeStep object in which low is overridden
     */
    public RangeStep withLow(Integer low) {
        return new RangeStep(scope, low, high);
    }
    
    /**
     * Construct a new immutable RangeStep object in which high is overridden
     */
    public RangeStep withHigh(Integer high) {
        return new RangeStep(scope, low, high);
    }
}
