package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

/**
 * @type string
 */
public class NotContaining {
    public final String value;
    
    /**
     * Constructs an immutable NotContaining object
     */
    public NotContaining(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NotContaining)) {
            return false;
        }
        NotContaining o = (NotContaining) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
