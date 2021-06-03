package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

/**
 * @type string
 */
public class Not {
    public final String value;
    
    /**
     * Constructs an immutable Not object
     */
    public Not(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Not)) {
            return false;
        }
        Not o = (Not) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
