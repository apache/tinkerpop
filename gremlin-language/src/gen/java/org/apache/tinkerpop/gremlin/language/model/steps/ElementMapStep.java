package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class ElementMapStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> propertyKeys;
    
    /**
     * Constructs an immutable ElementMapStep object
     */
    public ElementMapStep(java.util.List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ElementMapStep)) return false;
        ElementMapStep o = (ElementMapStep) other;
        return propertyKeys.equals(o.propertyKeys);
    }
    
    @Override
    public int hashCode() {
        return 2 * propertyKeys.hashCode();
    }
}
