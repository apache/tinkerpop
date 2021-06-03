package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

public class Outside {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral min;
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral max;
    
    /**
     * Constructs an immutable Outside object
     */
    public Outside(GenericLiteral min, GenericLiteral max) {
        this.min = min;
        this.max = max;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Outside)) {
            return false;
        }
        Outside o = (Outside) other;
        return min.equals(o.min)
            && max.equals(o.max);
    }
    
    @Override
    public int hashCode() {
        return 2 * min.hashCode()
            + 3 * max.hashCode();
    }
    
    /**
     * Construct a new immutable Outside object in which min is overridden
     */
    public Outside withMin(GenericLiteral min) {
        return new Outside(min, max);
    }
    
    /**
     * Construct a new immutable Outside object in which max is overridden
     */
    public Outside withMax(GenericLiteral max) {
        return new Outside(min, max);
    }
}
