package com.tinkerpop.gremlin.giraph.process.graph.step.map;

import com.tinkerpop.gremlin.giraph.hdfs.GiraphEdgeIterator;
import com.tinkerpop.gremlin.giraph.hdfs.GiraphVertexIterator;
import com.tinkerpop.gremlin.giraph.structure.GiraphGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphStep<E extends Element> extends GraphStep<E> {

    private final GiraphGraph graph;

    public GiraphGraphStep(final Traversal traversal, final Class<E> returnClass, final GiraphGraph graph) {
        super(traversal, returnClass);
        this.graph = graph;
    }

    public void clear() {
        this.starts.clear();
    }

    public void generateTraverserIterator(final boolean trackPaths) {
        this.starts.clear();
        try {
            this.starts.add(new TraverserIterator(this, trackPaths, Vertex.class.isAssignableFrom(this.returnClass) ? new GiraphVertexIterator(this.graph) : new GiraphEdgeIterator(this.graph)));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
