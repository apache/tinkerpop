package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class MathStep {
    /**
     * @type string
     */
    public final String expression;
    
    /**
     * Constructs an immutable MathStep object
     */
    public MathStep(String expression) {
        this.expression = expression;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MathStep)) return false;
        MathStep o = (MathStep) other;
        return expression.equals(o.expression);
    }
    
    @Override
    public int hashCode() {
        return 2 * expression.hashCode();
    }
}
