package com.tinkerpop.gremlin.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IdStep<E extends Element> extends MapStep<E, Object> {

    public IdStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> traverser.get().id());
    }
}
