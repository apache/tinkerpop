package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.TraversalScope;

public class SampleStep {
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalScope
     */
    public final java.util.Optional<TraversalScope> scope;
    
    /**
     * @type integer
     */
    public final Integer amountToSample;
    
    /**
     * Constructs an immutable SampleStep object
     */
    public SampleStep(java.util.Optional<TraversalScope> scope, Integer amountToSample) {
        this.scope = scope;
        this.amountToSample = amountToSample;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SampleStep)) {
            return false;
        }
        SampleStep o = (SampleStep) other;
        return scope.equals(o.scope)
            && amountToSample.equals(o.amountToSample);
    }
    
    @Override
    public int hashCode() {
        return 2 * scope.hashCode()
            + 3 * amountToSample.hashCode();
    }
    
    /**
     * Construct a new immutable SampleStep object in which scope is overridden
     */
    public SampleStep withScope(java.util.Optional<TraversalScope> scope) {
        return new SampleStep(scope, amountToSample);
    }
    
    /**
     * Construct a new immutable SampleStep object in which amountToSample is overridden
     */
    public SampleStep withAmountToSample(Integer amountToSample) {
        return new SampleStep(scope, amountToSample);
    }
}
