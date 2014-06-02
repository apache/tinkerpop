package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyValueStep<E> extends MapStep<Property, E> {

    public PropertyValueStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> (E) traverser.get().value());
    }
}
