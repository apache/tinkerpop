package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

import org.example.org.apache.tinkerpop.gremlin.language.model.literals.StringLiteral;

public class WithSack {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/literals.StringLiteral
     */
    public final StringLiteral initialValue;
    
    /**
     * @type optional: org/apache/tinkerpop/gremlin/language/model/traversal.TraversalOperator
     */
    public final java.util.Optional<TraversalOperator> operator;
    
    /**
     * Constructs an immutable WithSack object
     */
    public WithSack(StringLiteral initialValue, java.util.Optional<TraversalOperator> operator) {
        this.initialValue = initialValue;
        this.operator = operator;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof WithSack)) {
            return false;
        }
        WithSack o = (WithSack) other;
        return initialValue.equals(o.initialValue)
            && operator.equals(o.operator);
    }
    
    @Override
    public int hashCode() {
        return 2 * initialValue.hashCode()
            + 3 * operator.hashCode();
    }
    
    /**
     * Construct a new immutable WithSack object in which initialValue is overridden
     */
    public WithSack withInitialValue(StringLiteral initialValue) {
        return new WithSack(initialValue, operator);
    }
    
    /**
     * Construct a new immutable WithSack object in which operator is overridden
     */
    public WithSack withOperator(java.util.Optional<TraversalOperator> operator) {
        return new WithSack(initialValue, operator);
    }
}
