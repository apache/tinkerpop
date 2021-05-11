package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class InStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> edgeLabels;
    
    /**
     * Constructs an immutable InStep object
     */
    public InStep(java.util.List<String> edgeLabels) {
        this.edgeLabels = edgeLabels;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof InStep)) return false;
        InStep o = (InStep) other;
        return edgeLabels.equals(o.edgeLabels);
    }
    
    @Override
    public int hashCode() {
        return 2 * edgeLabels.hashCode();
    }
}
