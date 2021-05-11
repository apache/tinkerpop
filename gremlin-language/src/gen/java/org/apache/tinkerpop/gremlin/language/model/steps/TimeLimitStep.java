package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class TimeLimitStep {
    /**
     * @type integer
     */
    public final Integer timeLimit;
    
    /**
     * Constructs an immutable TimeLimitStep object
     */
    public TimeLimitStep(Integer timeLimit) {
        this.timeLimit = timeLimit;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TimeLimitStep)) return false;
        TimeLimitStep o = (TimeLimitStep) other;
        return timeLimit.equals(o.timeLimit);
    }
    
    @Override
    public int hashCode() {
        return 2 * timeLimit.hashCode();
    }
}
