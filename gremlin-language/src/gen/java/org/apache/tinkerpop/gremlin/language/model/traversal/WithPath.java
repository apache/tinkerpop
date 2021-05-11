package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public class WithPath {
    /**
     * Constructs an immutable WithPath object
     */
    public WithPath() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof WithPath)) return false;
        WithPath o = (WithPath) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
