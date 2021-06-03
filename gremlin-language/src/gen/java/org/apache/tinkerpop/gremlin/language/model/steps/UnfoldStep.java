package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class UnfoldStep {
    /**
     * Constructs an immutable UnfoldStep object
     */
    public UnfoldStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof UnfoldStep)) {
            return false;
        }
        UnfoldStep o = (UnfoldStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
