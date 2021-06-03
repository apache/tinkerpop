package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ValuesStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> propertyKeys;
    
    /**
     * Constructs an immutable ValuesStep object
     */
    public ValuesStep(java.util.List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValuesStep)) {
            return false;
        }
        ValuesStep o = (ValuesStep) other;
        return propertyKeys.equals(o.propertyKeys);
    }
    
    @Override
    public int hashCode() {
        return 2 * propertyKeys.hashCode();
    }
}
