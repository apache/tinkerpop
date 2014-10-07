package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LabelStep<E extends Element> extends MapStep<E, String> {

    public LabelStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> traverser.get().label());
    }
}
