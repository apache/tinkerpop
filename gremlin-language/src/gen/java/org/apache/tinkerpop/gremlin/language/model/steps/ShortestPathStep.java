package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ShortestPathStep {
    /**
     * Constructs an immutable ShortestPathStep object
     */
    public ShortestPathStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ShortestPathStep)) return false;
        ShortestPathStep o = (ShortestPathStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
