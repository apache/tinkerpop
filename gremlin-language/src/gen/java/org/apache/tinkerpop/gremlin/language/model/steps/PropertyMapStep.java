package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class PropertyMapStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> propertyKeys;
    
    /**
     * Constructs an immutable PropertyMapStep object
     */
    public PropertyMapStep(java.util.List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PropertyMapStep)) return false;
        PropertyMapStep o = (PropertyMapStep) other;
        return propertyKeys.equals(o.propertyKeys);
    }
    
    @Override
    public int hashCode() {
        return 2 * propertyKeys.hashCode();
    }
}
