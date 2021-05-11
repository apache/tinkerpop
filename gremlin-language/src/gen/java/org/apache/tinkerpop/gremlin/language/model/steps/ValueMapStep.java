package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ValueMapStep {
    /**
     * @type optional: boolean
     */
    public final java.util.Optional<Boolean> includeTokens;
    
    /**
     * @type list: string
     */
    public final java.util.List<String> propertyKeys;
    
    /**
     * Constructs an immutable ValueMapStep object
     */
    public ValueMapStep(java.util.Optional<Boolean> includeTokens, java.util.List<String> propertyKeys) {
        this.includeTokens = includeTokens;
        this.propertyKeys = propertyKeys;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueMapStep)) return false;
        ValueMapStep o = (ValueMapStep) other;
        return includeTokens.equals(o.includeTokens)
            && propertyKeys.equals(o.propertyKeys);
    }
    
    @Override
    public int hashCode() {
        return 2 * includeTokens.hashCode()
            + 3 * propertyKeys.hashCode();
    }
    
    /**
     * Construct a new immutable ValueMapStep object in which includeTokens is overridden
     */
    public ValueMapStep withIncludeTokens(java.util.Optional<Boolean> includeTokens) {
        return new ValueMapStep(includeTokens, propertyKeys);
    }
    
    /**
     * Construct a new immutable ValueMapStep object in which propertyKeys is overridden
     */
    public ValueMapStep withPropertyKeys(java.util.List<String> propertyKeys) {
        return new ValueMapStep(includeTokens, propertyKeys);
    }
}
