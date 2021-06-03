package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

/**
 * @type string
 */
public class NotEndingWith {
    public final String value;
    
    /**
     * Constructs an immutable NotEndingWith object
     */
    public NotEndingWith(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NotEndingWith)) {
            return false;
        }
        NotEndingWith o = (NotEndingWith) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
