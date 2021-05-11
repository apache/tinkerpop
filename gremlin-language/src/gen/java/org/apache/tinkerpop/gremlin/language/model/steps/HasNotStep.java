package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class HasNotStep {
    /**
     * @type string
     */
    public final String propertyKey;
    
    /**
     * Constructs an immutable HasNotStep object
     */
    public HasNotStep(String propertyKey) {
        this.propertyKey = propertyKey;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof HasNotStep)) return false;
        HasNotStep o = (HasNotStep) other;
        return propertyKey.equals(o.propertyKey);
    }
    
    @Override
    public int hashCode() {
        return 2 * propertyKey.hashCode();
    }
}
