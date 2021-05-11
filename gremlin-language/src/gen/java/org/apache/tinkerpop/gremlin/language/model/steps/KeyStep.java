package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class KeyStep {
    /**
     * Constructs an immutable KeyStep object
     */
    public KeyStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof KeyStep)) return false;
        KeyStep o = (KeyStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
