package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class IdStep {
    /**
     * Constructs an immutable IdStep object
     */
    public IdStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IdStep)) return false;
        IdStep o = (IdStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
