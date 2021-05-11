package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

/**
 * @type string
 */
public class StartingWith {
    public final String value;
    
    /**
     * Constructs an immutable StartingWith object
     */
    public StartingWith(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StartingWith)) return false;
        StartingWith o = (StartingWith) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
