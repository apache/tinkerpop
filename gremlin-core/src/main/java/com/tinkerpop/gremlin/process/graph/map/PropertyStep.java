package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyStep<E> extends MapStep<Element, Property<E>> {

    public PropertyStep(final Traversal traversal, final String key) {
        super(traversal);
        this.setFunction(traverser -> traverser.get().property(key));
    }
}
