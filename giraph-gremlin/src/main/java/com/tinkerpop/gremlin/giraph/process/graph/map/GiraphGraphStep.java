package com.tinkerpop.gremlin.giraph.process.graph.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.GraphStep;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphStep extends GraphStep {

    public GiraphGraphStep(Traversal traversal) {
        super(traversal, Vertex.class);
    }

    public void clear() {
    }

    public void generateHolderIterator(final boolean trackPaths) {

    }
}
