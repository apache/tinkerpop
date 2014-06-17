package com.tinkerpop.gremlin.giraph.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphStep<E extends Element> extends GraphStep<E> {

    public GiraphGraphStep(final Traversal traversal, final Class<E> returnClass) {
        super(traversal, returnClass);
    }

    public void clear() {
    }

    public void generateTraverserIterator(final boolean trackPaths) {

    }
}
