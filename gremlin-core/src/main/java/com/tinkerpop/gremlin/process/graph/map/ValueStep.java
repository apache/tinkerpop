package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ValueStep<E> extends MapStep<Property, E> {

    public ValueStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> (E) traverser.get().value());
    }
}
