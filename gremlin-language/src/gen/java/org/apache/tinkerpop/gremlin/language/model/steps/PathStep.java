package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class PathStep {
    /**
     * Constructs an immutable PathStep object
     */
    public PathStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PathStep)) return false;
        PathStep o = (PathStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
