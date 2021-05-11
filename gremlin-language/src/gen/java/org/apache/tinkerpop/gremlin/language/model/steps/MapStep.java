package org.example.org.apache.tinkerpop.gremlin.language.model.steps;

import org.example.org.apache.tinkerpop.gremlin.language.model.traversal.NestedTraversal;

public class MapStep {
    /**
     * @type org/apache/tinkerpop/gremlin/language/model/traversal.NestedTraversal
     */
    public final NestedTraversal mapTraversal;
    
    /**
     * Constructs an immutable MapStep object
     */
    public MapStep(NestedTraversal mapTraversal) {
        this.mapTraversal = mapTraversal;
    }
    
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MapStep)) return false;
        MapStep o = (MapStep) other;
        return mapTraversal.equals(o.mapTraversal);
    }
    
    @Override
    public int hashCode() {
        return 2 * mapTraversal.hashCode();
    }
}
