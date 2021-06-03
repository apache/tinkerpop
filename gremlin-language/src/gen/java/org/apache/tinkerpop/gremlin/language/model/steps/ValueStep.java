package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ValueStep {
    /**
     * Constructs an immutable ValueStep object
     */
    public ValueStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueStep)) {
            return false;
        }
        ValueStep o = (ValueStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
