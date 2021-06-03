package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

public class PageRankStep {
    /**
     * @type optional: float
     */
    public final java.util.Optional<Float> alpha;
    
    /**
     * Constructs an immutable PageRankStep object
     */
    public PageRankStep(java.util.Optional<Float> alpha) {
        this.alpha = alpha;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PageRankStep)) {
            return false;
        }
        PageRankStep o = (PageRankStep) other;
        return alpha.equals(o.alpha);
    }
    
    @Override
    public int hashCode() {
        return 2 * alpha.hashCode();
    }
}
