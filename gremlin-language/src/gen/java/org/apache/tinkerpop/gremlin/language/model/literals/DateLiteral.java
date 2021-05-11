package org.example.org.apache.tinkerpop.gremlin.language.model.literals;

/**
 * @type string
 */
public class DateLiteral {
    public final String value;
    
    /**
     * Constructs an immutable DateLiteral object
     */
    public DateLiteral(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DateLiteral)) return false;
        DateLiteral o = (DateLiteral) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
