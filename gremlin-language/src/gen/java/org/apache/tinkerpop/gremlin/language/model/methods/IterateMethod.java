package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public class IterateMethod {
    /**
     * Constructs an immutable IterateMethod object
     */
    public IterateMethod() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof IterateMethod)) {
            return false;
        }
        IterateMethod o = (IterateMethod) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
