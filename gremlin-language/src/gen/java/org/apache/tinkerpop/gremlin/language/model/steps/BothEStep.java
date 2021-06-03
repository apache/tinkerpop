package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class BothEStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> edgeLabels;
    
    /**
     * Constructs an immutable BothEStep object
     */
    public BothEStep(java.util.List<String> edgeLabels) {
        this.edgeLabels = edgeLabels;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof BothEStep)) {
            return false;
        }
        BothEStep o = (BothEStep) other;
        return edgeLabels.equals(o.edgeLabels);
    }
    
    @Override
    public int hashCode() {
        return 2 * edgeLabels.hashCode();
    }
}
