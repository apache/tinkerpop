package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class InVStep {
    /**
     * Constructs an immutable InVStep object
     */
    public InVStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof InVStep)) {
            return false;
        }
        InVStep o = (InVStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
