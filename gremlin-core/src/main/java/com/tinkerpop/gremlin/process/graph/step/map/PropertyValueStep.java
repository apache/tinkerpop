package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PropertyValueStep<E> extends MapStep<Property<E>, E> {

    public PropertyValueStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> traverser.get().value());
    }
}
