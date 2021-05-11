package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public class ToSetMethod {
    /**
     * Constructs an immutable ToSetMethod object
     */
    public ToSetMethod() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ToSetMethod)) return false;
        ToSetMethod o = (ToSetMethod) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
