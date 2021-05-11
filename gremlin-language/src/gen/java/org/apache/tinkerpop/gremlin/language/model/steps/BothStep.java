package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class BothStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> edgeLabels;
    
    /**
     * Constructs an immutable BothStep object
     */
    public BothStep(java.util.List<String> edgeLabels) {
        this.edgeLabels = edgeLabels;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BothStep)) return false;
        BothStep o = (BothStep) other;
        return edgeLabels.equals(o.edgeLabels);
    }
    
    @Override
    public int hashCode() {
        return 2 * edgeLabels.hashCode();
    }
}
