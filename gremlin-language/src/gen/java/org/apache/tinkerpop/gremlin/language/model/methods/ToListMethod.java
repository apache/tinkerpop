package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public class ToListMethod {
    /**
     * Constructs an immutable ToListMethod object
     */
    public ToListMethod() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ToListMethod)) {
            return false;
        }
        ToListMethod o = (ToListMethod) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
