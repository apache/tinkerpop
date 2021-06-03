package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class LabelStep {
    /**
     * Constructs an immutable LabelStep object
     */
    public LabelStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LabelStep)) {
            return false;
        }
        LabelStep o = (LabelStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
