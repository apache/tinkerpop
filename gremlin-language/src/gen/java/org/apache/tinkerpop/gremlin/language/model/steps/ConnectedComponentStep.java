package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ConnectedComponentStep {
    /**
     * Constructs an immutable ConnectedComponentStep object
     */
    public ConnectedComponentStep() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ConnectedComponentStep)) return false;
        ConnectedComponentStep o = (ConnectedComponentStep) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
