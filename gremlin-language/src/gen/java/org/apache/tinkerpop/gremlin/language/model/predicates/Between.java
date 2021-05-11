package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

public class Between {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral min;
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral max;
    
    /**
     * Constructs an immutable Between object
     */
    public Between(GenericLiteral min, GenericLiteral max) {
        this.min = min;
        this.max = max;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Between)) return false;
        Between o = (Between) other;
        return min.equals(o.min)
            && max.equals(o.max);
    }
    
    @Override
    public int hashCode() {
        return 2 * min.hashCode()
            + 3 * max.hashCode();
    }
    
    /**
     * Construct a new immutable Between object in which min is overridden
     */
    public Between withMin(GenericLiteral min) {
        return new Between(min, max);
    }
    
    /**
     * Construct a new immutable Between object in which max is overridden
     */
    public Between withMax(GenericLiteral max) {
        return new Between(min, max);
    }
}
