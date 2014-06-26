package com.tinkerpop.gremlin.giraph.process.graph.step.map;

import com.tinkerpop.gremlin.giraph.process.computer.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.structure.io.VertexIterator;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.GraphStep;
import com.tinkerpop.gremlin.process.util.TraverserIterator;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphGraphStep<E extends Element> extends GraphStep<E> {

    private final SConfiguration configuration;

    public GiraphGraphStep(final Traversal traversal, final Class<E> returnClass, final Configuration configuration) {
        super(traversal, returnClass);
        this.configuration = new SConfiguration(configuration);
    }

    public void clear() {
        this.starts.clear();
    }

    public void generateTraverserIterator(final boolean trackPaths) {
        this.starts.clear();
        try {
            final VertexIterator vertices = new VertexIterator(
                    (VertexInputFormat) configuration.getClass(GiraphGraphComputer.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, VertexInputFormat.class).getConstructor().newInstance(), this.configuration);
            if (trackPaths)
                this.starts.add(new TraverserIterator(this, Vertex.class.isAssignableFrom(this.returnClass) ? vertices : vertices));
            else
                this.starts.add(new TraverserIterator(Vertex.class.isAssignableFrom(this.returnClass) ? vertices : vertices));
        } catch (Exception e) {
            //e.printStackTrace();
            //throw new RuntimeException(e.getMessage(), e);
        }
    }

    public class SConfiguration extends Configuration implements Serializable {
        public SConfiguration() {
        }

        public SConfiguration(final Configuration configuration) {
            super(configuration);
        }
    }
}
