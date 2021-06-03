package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class CyclicPathStep {
    /**
     * Constructs an immutable CyclicPathStep object
     */
    public CyclicPathStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CyclicPathStep)) {
            return false;
        }
        CyclicPathStep o = (CyclicPathStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
