package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class SimplePathStep {
    /**
     * Constructs an immutable SimplePathStep object
     */
    public SimplePathStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SimplePathStep)) {
            return false;
        }
        SimplePathStep o = (SimplePathStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
