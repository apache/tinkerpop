package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class IndexStep {
    /**
     * Constructs an immutable IndexStep object
     */
    public IndexStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IndexStep)) return false;
        IndexStep o = (IndexStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
