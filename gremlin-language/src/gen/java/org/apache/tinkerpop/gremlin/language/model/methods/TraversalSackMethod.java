package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public class TraversalSackMethod {
    /**
     * Constructs an immutable TraversalSackMethod object
     */
    public TraversalSackMethod() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof TraversalSackMethod)) {
            return false;
        }
        TraversalSackMethod o = (TraversalSackMethod) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
