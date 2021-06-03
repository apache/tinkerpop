package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

/**
 * @type string
 */
public class Containing {
    public final String value;
    
    /**
     * Constructs an immutable Containing object
     */
    public Containing(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Containing)) {
            return false;
        }
        Containing o = (Containing) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
