package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class IdentityStep {
    /**
     * Constructs an immutable IdentityStep object
     */
    public IdentityStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IdentityStep)) return false;
        IdentityStep o = (IdentityStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
