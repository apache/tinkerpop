package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class PropertiesStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> propertyKeys;
    
    /**
     * Constructs an immutable PropertiesStep object
     */
    public PropertiesStep(java.util.List<String> propertyKeys) {
        this.propertyKeys = propertyKeys;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PropertiesStep)) return false;
        PropertiesStep o = (PropertiesStep) other;
        return propertyKeys.equals(o.propertyKeys);
    }
    
    @Override
    public int hashCode() {
        return 2 * propertyKeys.hashCode();
    }
}
