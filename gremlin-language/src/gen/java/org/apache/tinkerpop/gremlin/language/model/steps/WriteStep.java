package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class WriteStep {
    /**
     * Constructs an immutable WriteStep object
     */
    public WriteStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof WriteStep)) {
            return false;
        }
        WriteStep o = (WriteStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
