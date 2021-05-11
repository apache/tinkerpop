package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class BothVStep {
    /**
     * Constructs an immutable BothVStep object
     */
    public BothVStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BothVStep)) return false;
        BothVStep o = (BothVStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
