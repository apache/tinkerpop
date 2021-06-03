package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

/**
 * @type string
 */
public class NotStartingWith {
    public final String value;
    
    /**
     * Constructs an immutable NotStartingWith object
     */
    public NotStartingWith(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NotStartingWith)) {
            return false;
        }
        NotStartingWith o = (NotStartingWith) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
