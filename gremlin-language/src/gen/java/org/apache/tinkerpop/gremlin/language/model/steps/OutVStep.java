package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class OutVStep {
    /**
     * Constructs an immutable OutVStep object
     */
    public OutVStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof OutVStep)) return false;
        OutVStep o = (OutVStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
