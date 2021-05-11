package org.example.org.apache.tinkerpop.gremlin.language.model.literals;

/**
 * @type string
 */
public class NonEmptyStringLiteral {
    public final String value;
    
    /**
     * Constructs an immutable NonEmptyStringLiteral object
     */
    public NonEmptyStringLiteral(String value) {
        this.value = value;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof NonEmptyStringLiteral)) return false;
        NonEmptyStringLiteral o = (NonEmptyStringLiteral) other;
        return value.equals(o.value);
    }
    
    @Override
    public int hashCode() {
        return 2 * value.hashCode();
    }
}
