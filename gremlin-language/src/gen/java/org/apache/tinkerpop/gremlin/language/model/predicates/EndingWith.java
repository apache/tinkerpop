package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

/**
 * @type string
 */
public class EndingWith {
    public final String value;
    
    /**
     * Constructs an immutable EndingWith object
     */
    public EndingWith(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EndingWith)) return false;
        EndingWith o = (EndingWith) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
