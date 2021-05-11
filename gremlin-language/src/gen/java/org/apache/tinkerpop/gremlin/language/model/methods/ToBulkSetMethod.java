package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

public class ToBulkSetMethod {
    /**
     * Constructs an immutable ToBulkSetMethod object
     */
    public ToBulkSetMethod() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ToBulkSetMethod)) return false;
        ToBulkSetMethod o = (ToBulkSetMethod) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
