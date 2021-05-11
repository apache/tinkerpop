package org.example.org.apache.tinkerpop.gremlin.language.model.predicates;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.GenericLiteral;

public class Inside {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral min;
    
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.GenericLiteral
     */
    public final GenericLiteral max;
    
    /**
     * Constructs an immutable Inside object
     */
    public Inside(GenericLiteral min, GenericLiteral max) {
        this.min = min;
        this.max = max;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Inside)) return false;
        Inside o = (Inside) other;
        return min.equals(o.min)
            && max.equals(o.max);
    }
    
    @Override
    public int hashCode() {
        return 2 * min.hashCode()
            + 3 * max.hashCode();
    }
    
    /**
     * Construct a new immutable Inside object in which min is overridden
     */
    public Inside withMin(GenericLiteral min) {
        return new Inside(min, max);
    }
    
    /**
     * Construct a new immutable Inside object in which max is overridden
     */
    public Inside withMax(GenericLiteral max) {
        return new Inside(min, max);
    }
}
