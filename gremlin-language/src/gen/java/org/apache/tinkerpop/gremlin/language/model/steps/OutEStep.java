package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class OutEStep {
    /**
     * @type list: string
     */
    public final java.util.List<String> edgeLabels;
    
    /**
     * Constructs an immutable OutEStep object
     */
    public OutEStep(java.util.List<String> edgeLabels) {
        this.edgeLabels = edgeLabels;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof OutEStep)) {
            return false;
        }
        OutEStep o = (OutEStep) other;
        return edgeLabels.equals(o.edgeLabels);
    }
    
    @Override
    public int hashCode() {
        return 2 * edgeLabels.hashCode();
    }
}
