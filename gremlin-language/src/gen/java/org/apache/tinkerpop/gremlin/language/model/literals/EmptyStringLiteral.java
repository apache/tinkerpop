package org.example.org.apache.tinkerpop.gremlin.language.model.literals;

public class EmptyStringLiteral {
    /**
     * Constructs an immutable EmptyStringLiteral object
     */
    public EmptyStringLiteral() {}
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EmptyStringLiteral)) return false;
        EmptyStringLiteral o = (EmptyStringLiteral) other;
        return true;
    }
    
    @Override
    public int hashCode() {
        return 0;
    }
}
