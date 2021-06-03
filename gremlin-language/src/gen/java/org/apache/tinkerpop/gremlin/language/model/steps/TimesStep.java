package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class TimesStep {
    /**
     * @type integer
     */
    public final Integer maxLoops;
    
    /**
     * Constructs an immutable TimesStep object
     */
    public TimesStep(Integer maxLoops) {
        this.maxLoops = maxLoops;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TimesStep)) {
            return false;
        }
        TimesStep o = (TimesStep) other;
        return maxLoops.equals(o.maxLoops);
    }
    
    @Override
    public int hashCode() {
        return 2 * maxLoops.hashCode();
    }
}
