package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ReadStep {
    /**
     * Constructs an immutable ReadStep object
     */
    public ReadStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ReadStep)) return false;
        ReadStep o = (ReadStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
