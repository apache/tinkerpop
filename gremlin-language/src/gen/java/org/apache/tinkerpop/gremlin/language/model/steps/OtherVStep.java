package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class OtherVStep {
    /**
     * Constructs an immutable OtherVStep object
     */
    public OtherVStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof OtherVStep)) {
            return false;
        }
        OtherVStep o = (OtherVStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
