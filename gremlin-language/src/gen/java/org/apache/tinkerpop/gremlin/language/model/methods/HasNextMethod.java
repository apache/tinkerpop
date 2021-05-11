package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public class HasNextMethod {
    /**
     * Constructs an immutable HasNextMethod object
     */
    public HasNextMethod() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HasNextMethod)) return false;
        HasNextMethod o = (HasNextMethod) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
