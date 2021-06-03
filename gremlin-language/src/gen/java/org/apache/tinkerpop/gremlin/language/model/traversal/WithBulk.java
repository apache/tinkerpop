package org.example.org.apache.tinkerpop.gremlin.language.model.traversal;

public class WithBulk {
    /**
     * @type boolean
     */
    public final Boolean useBulk;
    
    /**
     * Constructs an immutable WithBulk object
     */
    public WithBulk(Boolean useBulk) {
        this.useBulk = useBulk;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof WithBulk)) {
            return false;
        }
        WithBulk o = (WithBulk) other;
        return useBulk.equals(o.useBulk);
    }
    
    @Override
    public int hashCode() {
        return 2 * useBulk.hashCode();
    }
}
