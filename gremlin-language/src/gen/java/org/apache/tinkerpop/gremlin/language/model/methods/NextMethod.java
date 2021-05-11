package org.example.org.apache.tinkerpop.gremlin.language.model.methods;

/**
 * @type optional: integer
 */
public class NextMethod {
    public final java.util.Optional<Integer> value;
    
    /**
     * Constructs an immutable NextMethod object
     */
    public NextMethod(java.util.Optional<Integer> value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NextMethod)) return false;
        NextMethod o = (NextMethod) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
