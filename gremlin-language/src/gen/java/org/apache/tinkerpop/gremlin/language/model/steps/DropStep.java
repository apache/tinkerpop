package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class DropStep {
    /**
     * Constructs an immutable DropStep object
     */
    public DropStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DropStep)) {
            return false;
        }
        DropStep o = (DropStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
